defmodule Postgrex.Replication do
  @moduledoc ~S"""
  API for logical replication in PostgreSQL.

  ## Prequisits

   1. `wal_level = logical` this can be verified by `SHOW wal_level` it should return `logical`

      ALTER SYSTEM SET wal_level=logical;
      ALTER SYSTEM SET max_wal_sender='10'
      ALTER SYSTEM SET maz_replication_slots='10'

   2. Creating a publication:

      CREATE PUBLICATION my_publication FOR ALL TABLES;


   3. CREATE_REPLICATION_SLOT my_slot LOGICAL test_decode;
   4. START_REPLICATION_SLOT my_slot LOGICAL 0/0;
  """

  use Connection
  require Logger

  alias Postgrex.Protocol

  @timeout 5_000

  defstruct [
    :protocol,
    idle_interval: 5000,
    reconnect_backoff: 500,
    subscribers: %{},
    subscription_slots: %{},
    connected: false,
    connect_opts: []
  ]

  @doc false
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @doc """
  """
  @spec start_link(Keyword.t()) :: {:ok, pid} | {:error, Postgrex.Error.t() | term}
  def start_link(opts) do
    connection_opts = Postgrex.Utils.default_opts(opts) ++ [replication: "database"]

    Connection.start_link(__MODULE__, connection_opts, [])
  end

  @doc """
  Subscribes to a replication slot using the `SUBSCRIBE` command.
  ## Options

    * `:wal_position` - Wal position to start streaming from (default: `{0, 0}`)
    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec subscribe(GenServer.server(), String.t(), Keyword.t()) :: {:ok, reference()}
  def subscribe(pid, subscription, opts \\ []) do
    wal_position = opts[:wal_position] || {0, 0}
    timeout = opts[:timeout] || @timeout

    Connection.call(pid, {:subscribe, subscription, wal_position}, timeout)
  end

  ## Callbacks

  def init(opts) do
    {:connect, :init, %__MODULE__{connect_opts: opts}}
  end

  def connect(_, state) do
    case Protocol.connect(state.connect_opts) do
      {:ok, protocol} -> {:ok, %{state | protocol: protocol, connected: true}}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  def handle_call({:subscribe, slot, wal_position}, {pid, _} = from, state) do
    ref = Process.monitor(pid)
    state = put_in(state.subscribers[ref], {{slot, wal_position}, pid})

    do_subscribe(slot, wal_position, pid, ref, from, state)
  end

  def handle_info(:timeout, %{protocol: protocol} = state) do
    # TODO Ping?
    {:noreply, state, state.idle_interval}
  end

  def handle_info(msg, s) do
    %{protocol: protocol, subscription_slots: slots, subscribers: subscribers} = s

    opts = [notify: &notify(slots, subscribers, "my_slot", &1)]

    case Protocol.handle_info(msg, opts, protocol) do
      {:ok, protocol} ->
        {:noreply, %{s | protocol: protocol}, s.idle_interval}

      {error, reason, protocol} ->
        Logger.error(fn -> "#{inspect(error)} #{inspect(reason)}" end)
        # reconnect_or_stop(error, reason, protocol, s)
        {:stop, reason, %{s | protocol: protocol}}
    end
  end

  defp do_subscribe(slot, {x, xx}, pid, ref, from, s) do
    s = update_in(s.subscription_slots[slot], &((&1 || %{}) |> Map.put(ref, pid)))

    stmt =
      "START_REPLICATION SLOT #{slot} LOGICAL #{x}/#{xx} (proto_version '1', publication_names 'postgrex')"

    replication_query(stmt, slot, from, s)
  end

  defp replication_query(
         statement,
         slot,
         from,
         %{protocol: p, subscription_slots: slots, subscribers: subscribers} = state
       ) do
    opts = [notify: &notify(slots, subscribers, slot, &1)]

    case Protocol.handle_replication(statement, opts, p) do
      {:ok, %Postgrex.Result{} = result, protocol} ->
        Connection.reply(from, result)
        checkin(protocol, state)

      {:error, reason, protocol} ->
        {:stop, reason, %{state | protocol: protocol}}
    end
  end

  defp notify(slots, subscribers, slot, msg) do
    Enum.each(Map.get(slots, slot) || [], fn {ref, _pid} ->
      {_, pid} = Map.fetch!(subscribers, ref)
      send(pid, {:replication, self(), ref, slot, msg})
    end)
  end

  defp checkin(protocol, s) do
    case Protocol.checkin(protocol) do
      {:ok, protocol} ->
        {:noreply, %{s | protocol: protocol}, s.idle_interval}

      {error, reason, protocol} ->
        {:stop, reason, %{s | protocol: protocol}}
    end
  end
end
