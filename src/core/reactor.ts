/**
 * Core of the reactor runtime.
 *
 * @author Marten Lohstroh (marten@berkeley.edu),
 * @author Matt Weber (matt.weber@berkeley.edu),
 * @author Hokeun Kim (hokeunkim@berkeley.edu)
 */

import {
  type Priority,
  type Absent,
  type ArgList,
  type Read,
  type Sched,
  type Variable,
  type Write,
  type TriggerManager,
  ReactionGraph,
  TimeValue,
  Tag,
  Origin,
  getCurrentPhysicalTime,
  Alarm,
  PrioritySet,
  Log,
  PrecedenceGraph,
  Reaction,
  Mutation,
  Procedure,
  SchedulableAction,
  TaggedEvent,
  Component,
  ScheduledTrigger,
  Trigger,
  Action,
  InPort,
  IOPort,
  MultiPort,
  OutPort,
  Port,
  WritablePort,
  Startup,
  Shutdown,
  WritableMultiPort,
  Dummy,
  ConnectablePort
} from "./internal";
import {v4 as uuidv4} from "uuid";
import {Bank} from "./bank";

// Set the default log level.
Log.setLevel(Log.LogLevel.ERROR);

// --------------------------------------------------------------------------//
// Interfaces                                                               //
// --------------------------------------------------------------------------//

/**
 * Interface for the invocation of remote procedures.
 */
export interface Call<A, R> extends Write<A>, Read<R> {
  invoke: (args: A) => R | undefined;
}

export enum CanConnectResult {
  SUCCESS = 0,
  SELF_LOOP = "Source port and destination port are the same.",
  DESTINATION_OCCUPIED = "Destination port is already occupied.",
  DOWNSTREAM_WRITE_CONFLICT = "Write conflict: port is already occupied.",
  NOT_IN_SCOPE = "Source and destination ports are not in scope.",
  RT_CONNECTION_OUTSIDE_CONTAINER = "New connection is outside of container.",
  RT_DIRECT_FEED_THROUGH = "New connection introduces direct feed through.",
  RT_CYCLE = "New connection introduces cycle.",
  MUTATION_CAUSALITY_LOOP = "New connection will change the causal effect of the mutation that triggered this connection."
}

/**
 * Abstract class for a schedulable action. It is intended as a wrapper for a
 * regular action. In addition to a get method, it also has a schedule method
 * that allows for the action to be scheduled.
 */

// --------------------------------------------------------------------------//
// Core Reactor Classes                                                     //
// --------------------------------------------------------------------------//

export class Parameter<T> implements Read<T> {
  constructor(private readonly value: T) {}

  get(): T {
    return this.value;
  }
}

/**
 * A timer is an attribute of a reactor which periodically (or just once)
 * creates a timer event. A timer has an offset and a period. Upon initialization
 * the timer will schedule an event at the given offset from starting wall clock time.
 * Whenever this timer's event comes off the event queue, it will
 * reschedule the event at the current logical time + period in the future. A 0
 * period indicates the timer's event is a one-off and should not be rescheduled.
 */
export class Timer extends ScheduledTrigger<Tag> implements Read<Tag> {
  period: TimeValue;

  offset: TimeValue;

  /**
   * Timer constructor.
   * @param __container__ The reactor this timer is attached to.
   * @param offset The interval between the start of execution and the first
   * timer event. Cannot be negative.
   * @param period The interval between rescheduled timer events. If 0, will
   * not reschedule. Cannot be negative.
   */
  constructor(
    __container__: Reactor,
    offset: TimeValue | 0,
    period: TimeValue | 0
  ) {
    super(__container__);

    if (!(offset instanceof TimeValue)) {
      this.offset = TimeValue.secs(0);
    } else {
      this.offset = offset;
    }

    if (!(period instanceof TimeValue)) {
      this.period = TimeValue.secs(0);
    } else {
      this.period = period;
    }
    Log.debug(this, () => "Creating timer: " + this._getFullyQualifiedName());
    // Initialize this timer.
    this.runtime.initialize(this);
  }

  public toString(): string {
    return `Timer from ${this._getContainer()._getFullyQualifiedName()} 
            with period: ${this.period} 
            offset: ${this.offset}`;
  }

  public get(): Tag | Absent {
    if (this.isPresent()) {
      return this.tag;
    } else {
      return undefined;
    }
  }
}

/**
 * A reactor is a software component that reacts to input events, timer events,
 * and action events. It has private state variables that are not visible to any
 * other reactor. Its reactions can consist of altering its own state, sending
 * messages to other reactors, or affecting the environment through some kind of
 * actuation or side effect.
 */
export abstract class Reactor extends Component {
  /**
   * Data structure to keep track of registered components.
   * Note: declare this class member before any other ones as they may
   * attempt to access it.
   */
  private readonly _keyChain = new Map<Component, symbol>();

  /**
   * This graph has in it all the dependencies implied by this container's
   * ports, reactions, and connections.
   */
  protected _dependencyGraph = new PrecedenceGraph<
    Port<unknown> | Reaction<Variable[]>
  >();

  /**
   * The runtime object, which has a collection of privileged functions that are passed down from the
   * container.
   */
  private _runtime!: Runtime;

  /**
   * Index that specifies the location of the reactor instance in a bank,
   * if it is a member of one.
   */
  private readonly _bankIndex: number;

  /**
   * Return the location of the reactor instance in a bank,
   * if it is a member of one; return -1 otherwise.
   */
  public getBankIndex(): number {
    if (this._bankIndex === undefined) {
      return -1;
    }
    return this._bankIndex;
  }

  /**
   * This graph has some overlap with the reactors dependency graph, but is
   * different in two respects:
   * - transitive dependencies between ports have been collapsed; and
   * - it incorporates the causality interfaces of all contained reactors.
   * It thereby carries enough information to find out whether adding a new
   * connection at runtime could result in a cyclic dependency, _without_
   * having to consult other reactors.
   */
  private readonly _causalityGraph = new PrecedenceGraph<Port<unknown>>();

  /**
   * Indicates whether this reactor is active (meaning it has reacted to a
   * startup action), or not (in which case it either never started up or has
   * reacted to a shutdown action).
   */
  protected _active = false;

  /**
   * This reactor's shutdown action.
   */
  readonly shutdown: Shutdown;

  /**
   * This reactor's startup action.
   */
  readonly startup: Startup;

  /**
   * This reactor's dummy action.
   */
  readonly __dummy: Dummy;

  /**
   * The list of reactions this reactor has.
   */
  private readonly _reactions: Array<Reaction<Variable[]>> = [];

  /**
   * Sandbox for the execution of reactions.
   */
  private _reactionScope: ReactionSandbox;

  /**
   * The list of mutations this reactor has.
   */
  private readonly _mutations: Array<Mutation<Variable[]>> = [];

  /**
   * Sandbox for the execution of mutations.
   */
  private _mutationScope: MutationSandbox;

  /**
   * Receive the runtime object from the container of this reactor.
   * Invoking this method in any user-written code will result in a
   * runtime error.
   * @param runtime The runtime object handed down from the container.
   */
  public _receiveRuntimeObject(runtime: Runtime): void {
    if (this._runtime == null && runtime != null) {
      this._runtime = runtime;
      // In addition to setting the runtime object, also make its
      // utility functions available as a protected member.
      this.util = runtime.util;
    } else {
      throw new Error("Can only establish link to runtime once.");
    }
  }

  /**
   * Add a component to this container.
   *
   * A component that is created as part of this reactor invokes this method
   * upon creation.
   * @param component The component to register.
   * @param key The component's key.
   */
  public _register(component: Component, key: symbol): void {
    if (component === undefined || component === null) {
      throw new Error("Unable to register undefined or null component");
    }
    if (component._isRegistered()) {
      throw new Error(
        "Unable to register " +
          component._getFullyQualifiedName() +
          " as it already has a container."
      );
    }
    // Only add key if the component isn't a self-reference
    // and isn't already registered.
    if (component !== this && !this._keyChain.has(component)) {
      this._keyChain.set(component, key);
    }
  }

  public _requestRuntimeObject(component: Component): void {
    if (component._isContainedBy(this)) {
      component._receiveRuntimeObject(this._runtime);
    }
  }

  /**
   * Remove all the connections associated with a given reactor.
   * @param reactor
   */
  private _deleteConnections(reactor: Reactor): void {
    for (const port of reactor._findOwnPorts()) {
      this._dependencyGraph.removeNode(port);
    }
  }

  /**
   * Remove this reactor from its container and sever any connections it may
   * still have. This reactor will become defunct and is ready for garbage
   * collection.
   */
  protected _unplug(): void {
    this._getContainer()._deregister(this, this._key);
  }

  /**
   * Remove the given reactor and its connections from this container if
   * the key matches.
   * @param reactor
   * @param key
   */
  public _deregister(reactor: Reactor, key: symbol): void {
    let found;
    for (const v of this._keyChain.values()) {
      if (v === key) {
        found = true;
        break;
      }
    }
    if (found ?? false) {
      this._keyChain.delete(reactor);
      this._deleteConnections(reactor);
    } else {
      console.log(
        "Unable to deregister reactor: " + reactor._getFullyQualifiedName()
      );
    }
  }

  private _getLast(
    reactions: Set<Reaction<Variable[]>>
  ): Reaction<Variable[]> | undefined {
    let index = -1;
    const all = this._getReactionsAndMutations();

    for (const reaction of reactions) {
      const found = all.findIndex((r) => r === reaction);
      if (found >= 0) {
        index = Math.max(found, index);
      }
    }
    if (index >= 0) {
      return all[index];
    }
  }

  private _getFirst(
    reactions: Set<Reaction<Variable[]>>
  ): Reaction<Variable[]> | undefined {
    let index = -1;
    const all = this._getReactionsAndMutations();

    for (const reaction of reactions) {
      const found = all.findIndex((r) => r === reaction);
      if (found >= 0) {
        index = Math.min(found, index);
      }
    }
    if (index >= 0) {
      return all[index];
    }
  }

  /**
   * If the given component is owned by this reactor, look up its key and
   * return it. Otherwise, if a key has been provided, and it matches the
   * key of this reactor, also look up the component's key and return it.
   * Otherwise, if the component is owned by a reactor that is owned by
   * this reactor, request the component's key from that reactor and return
   * it. If the component is an action, this request is not honored because
   * actions are never supposed to be accessed across levels of hierarchy.
   * @param component The component to look up the key for.
   * @param key The key that verifies the containment relation between this
   * reactor and the component, with at most one level of indirection.
   */
  public _getKey(component: Trigger, key?: symbol): symbol | undefined {
    if (component._isContainedBy(this) || this._key === key) {
      return this._keyChain.get(component);
    } else if (
      !(component instanceof Action) &&
      component._isContainedByContainerOf(this)
    ) {
      const owner = component.getContainer();
      if (owner !== null) {
        return owner._getKey(component, this._keyChain.get(owner));
      }
    }
  }

  /**
   * Collection of utility functions for this reactor and its subclasses.
   */
  protected util!: UtilityFunctions;

  /**
   * Mark this reactor for deletion, trigger all of its shutdown reactions
   * and mutations, and also delete all of the reactors that this reactor
   * contains.
   */
  private _delete(): void {
    // console.log("Marking for deletion: " + this._getFullyQualifiedName())
    this._runtime.delete(this);
    this.shutdown.update(
      new TaggedEvent(this.shutdown, this.util.getCurrentTag(), null)
    );
    // this._findOwnReactors().forEach(r => r._delete())
  }

  /**
   * Inner class intended to provide access to methods that should be
   * accessible to mutations, not to reactions.
   */
  private readonly _mutationSandbox = class implements MutationSandbox {
    public util: UtilityFunctions;

    constructor(private readonly reactor: Reactor) {
      this.reactor = reactor;
      this.util = reactor.util;
      this.getBankIndex = () => reactor.getBankIndex();
    }

    getBankIndex: () => number;

    /**
     *
     * @param src
     * @param dst
     */

    public connect<R, S extends R>(
      src: ConnectablePort<S>,
      dst: ConnectablePort<R>
    ): void;
    public connect<A extends T, R, T, S extends R>(
      src: CallerPort<A, R>,
      dst: CalleePort<T, S>
    ): void;
    public connect<A extends T, R, T, S extends R>(
      ...[src, dst]:
        | [ConnectablePort<S>, ConnectablePort<R>]
        | [CallerPort<A, R>, CalleePort<T, S>]
    ): void {
      if (src instanceof CallerPort && dst instanceof CalleePort) {
        this.reactor._connectCall(src, dst);
      } else if (
        src instanceof ConnectablePort &&
        dst instanceof ConnectablePort
      ) {
        this.reactor._connect(src.getPort(), dst.getPort());
      } else {
        throw Error(
          "Logically unreachable code: src and dst type mismatch, Caller(ee) port cannot be connected to IOPort."
        );
      }
    }

    public disconnect<R, S extends R>(src: IOPort<S>, dst?: IOPort<R>): void {
      if (
        src instanceof IOPort &&
        (dst === undefined || dst instanceof IOPort)
      ) {
        this.reactor._disconnect(src, dst);
      } else {
        // FIXME: Add an error reporting mechanism such as an exception.
      }
    }

    /**
     * Return the reactor containing the mutation using this sandbox.
     */
    public getReactor(): Reactor {
      return this.reactor;
    }

    /**
     * Mark the given reactor for deletion.
     *
     * @param reactor
     */
    public delete(reactor: Reactor): void {
      reactor._delete();
    }
  };

  /**
   * Inner class that furnishes an execution environment for reactions.
   */
  private readonly _reactionSandbox = class implements ReactionSandbox {
    public util: UtilityFunctions;

    public getBankIndex: () => number;

    constructor(public reactor: Reactor) {
      this.util = reactor.util;
      this.getBankIndex = () => reactor.getBankIndex();
    }
  };

  /**
   * Create a new reactor.
   * @param container The container of this reactor.
   */
  constructor(container: Reactor | null) {
    super(container);
    this._bankIndex = -1;
    if (container !== null) {
      const index = Bank.initializationMap.get(container);
      if (index !== undefined) {
        this._bankIndex = index;
      }
    }

    this._linkToRuntimeObject();
    this.shutdown = new Shutdown(this);
    this.startup = new Startup(this);
    this.__dummy = new Dummy(this);

    // Utils get passed down the hierarchy. If this is an App,
    // the container refers to this object, making the following
    // assignment idemponent.
    // this.util = this._getContainer().util

    // Create sandboxes for the reactions and mutations to execute in.
    this._reactionScope = new this._reactionSandbox(this);
    this._mutationScope = new this._mutationSandbox(this);

    // Pass in a reference to the reactor because the runtime object
    // is inaccessible for the top-level reactor (it is created after this constructor returns).
    const self = this as Reactor;
    this.addMutation([this.shutdown], [], function (this) {
      self._findOwnReactors().forEach((r) => {
        r._delete();
      });
    });

    // If this reactor was created at runtime, simply set the priority of
    // the default to the priority of from the last mutation of its
    // container plus one. Subsequent reactions and mutations that are added
    // will get a priority relative to this one.
    // FIXME: If any of the assigned priorities is larger than any downstream
    // reaction, then the priorities of those downstream reactions must be
    // increased.
    if (!(this instanceof App) && this._runtime.isRunning()) {
      const toDependOn = this._getContainer()._getLastMutation();
      if (toDependOn != null)
        this._mutations[0].setPriority(toDependOn.getPriority() + 1);
    }
  }

  protected _initializeReactionScope(): void {
    this._reactionScope = new this._reactionSandbox(this);
  }

  protected _initializeMutationScope(): void {
    this._mutationScope = new this._mutationSandbox(this);
  }

  // protected _isActive(): boolean {
  //     return this._active
  // }

  //

  public allWritable<T>(port: MultiPort<T>): WritableMultiPort<T> {
    return port.asWritable(this._getKey(port));
  }

  public writable<T>(port: IOPort<T>): WritablePort<T> {
    return port.asWritable(this._getKey(port));
  }

  /**
   * Return the index of the reaction given as an argument.
   * @param reaction The reaction to return the index of.
   */
  public _getReactionIndex(reaction: Reaction<Variable[]>): number {
    let index: number | undefined;

    if (reaction instanceof Mutation) {
      index = this._mutations.indexOf(reaction as Mutation<Variable[]>);
    } else {
      index = this._reactions.indexOf(reaction);
    }

    if (index !== undefined) return index;

    throw new Error("Reaction is not listed.");
  }

  protected schedulable<T>(action: Action<T>): Sched<T> {
    return action.asSchedulable(this._getKey(action));
  }

  private _recordDeps<T extends Variable[]>(reaction: Reaction<T>): void {
    // Add a dependency on the previous reaction or mutation, if it exists.
    const prev = this._getLastReactionOrMutation();
    if (prev != null) {
      this._dependencyGraph.addEdge(
        prev,
        reaction as unknown as Reaction<Variable[]>
      );
    }

    // FIXME: Add a dependency on the last mutation that the owner of this reactor
    // has. How do we know that it is the last? We have a "lastCaller" problem here.
    // Probably better to solve this at the level of the dependency graph with a function
    // that allows for a link to be updated.

    // Set up the triggers.
    for (const t of reaction.trigs) {
      // Link the trigger to the reaction.
      if (t instanceof Trigger) {
        t.getManager(this._getKey(t)).addReaction(
          reaction as unknown as Reaction<Variable[]>
        );
      } else if (t instanceof Array) {
        t.forEach((trigger) => {
          if (trigger instanceof Trigger) {
            trigger
              .getManager(this._getKey(trigger))
              .addReaction(reaction as unknown as Reaction<Variable[]>);
          } else {
            throw new Error("Non-Trigger included in Triggers list.");
          }
        });
      }

      // Also record this trigger as a dependency.
      if (t instanceof IOPort) {
        this._dependencyGraph.addEdge(
          t,
          reaction as unknown as Reaction<Variable[]>
        );
      } else if (t instanceof MultiPort) {
        t.channels().forEach((channel) => {
          this._dependencyGraph.addEdge(
            channel,
            reaction as unknown as Reaction<Variable[]>
          );
        });
      } else if (t instanceof Array) {
        t.forEach((trigger) => {
          if (trigger instanceof IOPort) {
            this._dependencyGraph.addEdge(
              trigger,
              reaction as unknown as Reaction<Variable[]>
            );
          } else if (trigger instanceof MultiPort) {
            trigger.channels().forEach((channel) => {
              this._dependencyGraph.addEdge(
                channel,
                reaction as unknown as Reaction<Variable[]>
              );
            });
          } else {
            throw new Error("Non-Port included in Triggers list.");
          }
        });
      } else {
        Log.debug(
          this,
          () => `
        >>>>> not a dependency: ${t}`
        );
      }
    }

    const sources = new Set<Port<unknown>>();
    const effects = new Set<Port<unknown>>();

    for (const a of reaction.args) {
      if (a instanceof IOPort) {
        this._dependencyGraph.addEdge(
          a,
          reaction as unknown as Reaction<Variable[]>
        );
        sources.add(a);
      } else if (a instanceof MultiPort) {
        a.channels().forEach((channel) => {
          this._dependencyGraph.addEdge(
            channel,
            reaction as unknown as Reaction<Variable[]>
          );
          sources.add(channel);
        });
      } else if (a instanceof CalleePort) {
        this._dependencyGraph.addEdge(
          reaction as unknown as Reaction<Variable[]>,
          a
        );
      } else if (a instanceof CallerPort) {
        this._dependencyGraph.addEdge(
          a,
          reaction as unknown as Reaction<Variable[]>
        );
      }
      // Only necessary if we want to add actions to the dependency graph.
      else if (a instanceof Action) {
        // dep
      } else if (a instanceof SchedulableAction) {
        // antidep
      } else if (a instanceof WritablePort) {
        this._dependencyGraph.addEdge(
          reaction as unknown as Reaction<Variable[]>,
          a.getPort()
        );
        effects.add(a.getPort());
      } else if (a instanceof WritableMultiPort) {
        a.getPorts().forEach((channel) => {
          this._dependencyGraph.addEdge(
            reaction as unknown as Reaction<Variable[]>,
            channel
          );
          effects.add(channel);
        });
      }
    }
    // Make effects dependent on sources.
    for (const effect of effects) {
      for (const source of sources) {
        this._causalityGraph.addEdge(source, effect);
      }
    }
  }

  /**
   * Given a reaction, return the reaction within this reactor that directly
   * precedes it, or `undefined` if there is none.
   * @param reaction A reaction to find the predecessor of.
   */
  protected prevReaction(
    reaction: Reaction<Variable[]>
  ): Reaction<Variable[]> | undefined {
    let index: number | undefined;

    if (reaction instanceof Mutation) {
      index = this._mutations.indexOf(reaction as Mutation<Variable[]>);
      if (index !== undefined && index > 0) {
        return this._mutations[index - 1];
      }
    } else {
      index = this._reactions.indexOf(reaction);
      if (index !== undefined && index > 0) {
        return this._reactions[index - 1];
      } else {
        const len = this._mutations.length;
        if (len > 0) {
          return this._mutations[len - 1];
        }
      }
    }
  }

  /**
   * Given a reaction, return the reaction within this reactior that directly
   * succeeds it, or `undefined` if there is none.
   * @param reaction A reaction to find the successor of.
   */
  protected nextReaction(
    reaction: Reaction<Variable[]>
  ): Reaction<Variable[]> | undefined {
    let index: number | undefined;

    if (reaction instanceof Mutation) {
      index = this._mutations.indexOf(reaction as Mutation<Variable[]>);
      if (index !== undefined && index < this._mutations.length - 1) {
        return this._mutations[index + 1];
      } else if (this._reactions.length > 0) {
        return this._reactions[0];
      }
    } else {
      index = this._reactions.indexOf(reaction);
      if (index !== undefined && index < this._reactions.length - 1) {
        return this._reactions[index + 1];
      }
    }
  }

  /**
   * Add a reaction to this reactor. Each newly added reaction will acquire a
   * dependency either on the previously added reaction, or on the last added
   * mutation (in case no reactions had been added prior to this one). A
   * reaction is specified by a list of triggers, a list of arguments, a react
   * function, an optional deadline, and an optional late function (which
   * represents the reaction body of the deadline). All triggers a reaction
   * needs access must be included in the arguments.
   *
   * @param trigs
   * @param args
   * @param react
   * @param deadline
   * @param late
   */
  protected addReaction<T extends Variable[]>(
    trigs: Variable[],
    args: [...ArgList<T>],
    react: (this: ReactionSandbox, ...args: ArgList<T>) => void,
    deadline?: TimeValue,
    late: (this: ReactionSandbox, ...args: ArgList<T>) => void = () => {
      Log.globalLogger.warn("Deadline violation occurred!");
    }
  ): void {
    const calleePorts = trigs.filter((trig) => trig instanceof CalleePort);

    if (calleePorts.length > 0) {
      // This is a procedure.
      const port = calleePorts[0] as CalleePort<unknown, unknown>;
      const procedure = new Procedure(
        this,
        this._reactionScope,
        trigs,
        args,
        react,
        deadline,
        late
      );
      if (trigs.length > 1) {
        // A procedure can only have a single trigger.
        throw new Error(`Procedure "${procedure}" has multiple triggers.`);
      }
      procedure.active = true;
      this._recordDeps(procedure);
      // Let the last caller point to the reaction that precedes this one.
      // This lets the first caller depend on it.
      port
        .getManager(this._getKey(port))
        .setLastCaller(this._getLastReactionOrMutation());
      this._reactions.push(procedure as unknown as Procedure<Variable[]>);
      // FIXME: set priority manually if this happens at runtime.
    } else {
      // This is an ordinary reaction.
      const reaction = new Reaction(
        this,
        this._reactionScope,
        trigs,
        args,
        react,
        deadline,
        late
      );
      // Stage it directly if it to be triggered immediately.
      if (reaction.isTriggeredImmediately()) {
        this._runtime.stage(reaction as unknown as Reaction<Variable[]>);
        // FIXME: if we're already running, then we need to set the priority as well.
      }
      reaction.active = true;
      this._recordDeps(reaction);
      this._reactions.push(reaction as unknown as Reaction<Variable[]>);
      // FIXME: set priority manually if this happens at runtime.
    }
  }

  protected addMutation<T extends Variable[]>(
    trigs: Variable[],
    args: [...ArgList<T>],
    react: (this: MutationSandbox, ...args: ArgList<T>) => void,
    deadline?: TimeValue,
    late: (this: MutationSandbox, ...args: ArgList<T>) => void = () => {
      Log.globalLogger.warn("Deadline violation occurred!");
    }
  ): void {
    const mutation = new Mutation(
      this,
      this._mutationScope,
      trigs,
      args,
      react,
      deadline,
      late
    );
    // Stage it directly if it to be triggered immediately.
    if (mutation.isTriggeredImmediately()) {
      this._runtime.stage(mutation as unknown as Mutation<Variable[]>);
    }
    mutation.active = true;
    this._recordDeps(mutation);
    this._mutations.push(mutation as unknown as Mutation<Variable[]>);
  }

  private _addHierarchicalDependencies(): void {
    const dependent = this._getFirstReactionOrMutation();
    const toDependOn = this._getContainer()._getLastMutation();
    if (
      dependent != null &&
      toDependOn != null &&
      this._getContainer() !== this
    ) {
      this._dependencyGraph.addEdge(toDependOn, dependent); // FIXME: this assumes there is always at least one mutation.
    }
  }

  private _addRPCDependencies(): void {
    // FIXME: Potentially do this in connect instead upon connecting to a
    // callee port. So far, it is unclear how RPCs would work when
    // established at runtime by a mutation.
    //
    // Check if there are any callee ports owned by this reactor.
    // If there are, add a dependency from its last caller to the antidependencies
    // of the procedure (excluding the callee port itself).
    const calleePorts = this._findOwnCalleePorts();
    for (const p of calleePorts) {
      const procedure = p.getManager(this._getKey(p)).getProcedure();
      const lastCaller = p.getManager(this._getKey(p)).getLastCaller();
      if (procedure != null && lastCaller != null) {
        const effects = this._dependencyGraph.getDownstreamNeighbors(procedure);
        for (const e of effects) {
          if (!(e instanceof CalleePort)) {
            // Also add edge to the local graph.
            this._dependencyGraph.addEdge(lastCaller, e);
          }
        }
      } else {
        Error("No procedure");
      }
    }
  }

  /**
   * Recursively collect the local dependency graph of each contained reactor
   * and merge them all in one graph.
   *
   * The recursion depth can be limited via the depth parameter. A depth of 0
   * will only return the local dependency graph of this reactor, a depth
   * of 1 will merge the local graph only with this reactor's immediate
   * children, etc. The default dept is -1, which will let this method
   * recurse until it has reached a reactor with no children.
   *
   * Some additional constraits are added to guarantee the following:
   *  - The first reaction or mutation has a dependency on the last mutation
   *    of this reactor's container; and
   *  - RPCs occur in a deterministic order.
   * @param depth The depth of recursion.
   */
  protected _getPrecedenceGraph(
    depth = -1
  ): PrecedenceGraph<Port<unknown> | Reaction<Variable[]>> {
    const graph = new PrecedenceGraph<Port<unknown> | Reaction<Variable[]>>();

    this._addHierarchicalDependencies();
    this._addRPCDependencies();

    graph.addAll(this._dependencyGraph);

    if (depth > 0 || depth < 0) {
      if (depth > 0) {
        depth--;
      }
      for (const r of this._getOwnReactors()) {
        graph.addAll(r._getPrecedenceGraph(depth));
      }
    }

    return graph;
  }

  /**
   * Return the reactors that this reactor owns.
   */
  private _getOwnReactors(): Reactor[] {
    // eslint-disable-next-line
    return Array.from(this._keyChain.keys()).filter(
      (it) => it instanceof Reactor
    ) as Reactor[];
  }

  /**
   * Return a list of reactions owned by this reactor.
   */
  protected _getReactions(): Array<Reaction<Variable[]>> {
    const arr = new Array<Reaction<Variable[]>>();
    this._reactions.forEach((it) => arr.push(it));
    return arr;
  }

  /**
   * Return a list of reactions and mutations owned by this reactor.
   */
  protected _getReactionsAndMutations(): Array<Reaction<Variable[]>> {
    const arr = new Array<Reaction<Variable[]>>();
    this._mutations.forEach((it) => arr.push(it));
    this._reactions.forEach((it) => arr.push(it));
    return arr;
  }

  /**
   * Return the last mutation of this reactor. All contained reactors
   * must have their reactions depend on this.
   */
  protected _getLastMutation(): Mutation<Variable[]> | undefined {
    const len = this._mutations.length;
    if (len > 0) {
      return this._mutations[len - 1];
    }
  }

  protected _getFirstReactionOrMutation(): Reaction<Variable[]> | undefined {
    if (this._mutations.length > 0) {
      return this._mutations[0];
    }
    if (this._reactions.length > 0) {
      return this._reactions[0];
    }
  }

  /**
   * Return the last reaction or mutation of this reactor.
   */
  protected _getLastReactionOrMutation(): Reaction<Variable[]> | undefined {
    let len = this._reactions.length;
    if (len > 0) {
      return this._reactions[len - 1];
    }
    len = this._mutations.length;
    if (len > 0) {
      return this._mutations[len - 1];
    }
  }

  /**
   * Return a list of reactions owned by this reactor.
   *
   * The returned list is a copy of the list kept inside of the reactor,
   * so changing it will not affect this reactor.
   */
  protected _getMutations(): Array<Reaction<Variable[]>> {
    const arr = new Array<Reaction<Variable[]>>();
    this._mutations.forEach((it) => arr.push(it));
    return arr;
  }

  /**
   * Report whether the given port is downstream of this reactor. If so, the
   * given port can be connected to with an output port of this reactor.
   * @param port
   */
  public _isDownstream(port: Port<unknown>): boolean {
    if (port instanceof InPort) {
      if (port._isContainedByContainerOf(this)) {
        return true;
      }
    }
    if (port instanceof OutPort) {
      if (port._isContainedBy(this)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Report whether the given port is upstream of this reactor. If so, the
   * given port can be connected to an input port of this reactor.
   * @param port
   */
  public _isUpstream(port: Port<unknown>): boolean {
    if (port instanceof OutPort) {
      if (port._isContainedByContainerOf(this)) {
        return true;
      }
    }
    if (port instanceof InPort) {
      if (port._isContainedBy(this)) {
        return true;
      }
    }
    return false;
  }

  public canConnectCall<A extends T, R, T, S extends R>(
    src: CallerPort<A, R>,
    dst: CalleePort<T, S>
  ): boolean {
    // FIXME: can we change the inheritance relationship so that we can overload?

    if (!this._runtime.isRunning()) {
      // console.log("Connecting before running")
      // Validate connections between callers and callees.

      if (
        src._isContainedByContainerOf(this) &&
        dst._isContainedByContainerOf(this)
      ) {
        return true;
      }
      return false;
    } else {
      // FIXME
    }
    return false;
  }

  /**
   * Returns true if a given source port can be connected to the
   * given destination port, false otherwise. Valid connections
   * must:
   * (1) adhere to the scoping rules and connectivity constraints
   *     of reactors; and
   * (2) not introduce cycles.
   *
   * The scoping rules of reactors can be summarized as follows:
   *  - A port cannot connect to itself;
   *  - Unless the connection is between a caller and callee, the
   *    destination can only be connected to one source;
   *  - ...
   * @param src The start point of a new connection.
   * @param dst The end point of a new connection.
   */
  public canConnect<R, S extends R>(
    src: IOPort<S>,
    dst: IOPort<R>
  ): CanConnectResult {
    // Immediate rule out trivial self loops.
    if (src === dst) {
      return CanConnectResult.SELF_LOOP;
    }

    // Check the race condition
    //   - between reactors and reactions (NOTE: check also needs to happen
    //     in addReaction)
    const deps = this._dependencyGraph.getUpstreamNeighbors(dst); // FIXME this will change with multiplex ports
    if (deps !== undefined && deps.size > 0) {
      return CanConnectResult.DESTINATION_OCCUPIED;
    }

    if (!this._runtime.isRunning()) {
      // console.log("Connecting before running")
      // Validate connections between callers and callees.
      // Additional checks for regular ports.

      // console.log("IOPort")
      // Rule out write conflicts.
      //   - (between reactors)
      if (this._dependencyGraph.getDownstreamNeighbors(dst).size > 0) {
        return CanConnectResult.DOWNSTREAM_WRITE_CONFLICT;
      }

      if (!this._isInScope(src, dst)) {
        return CanConnectResult.NOT_IN_SCOPE;
      }
      return CanConnectResult.SUCCESS;
    } else {
      // Attempt to make a connection while executing.
      // Check the local dependency graph to figure out whether this change
      // introduces zero-delay feedback.
      // console.log("Runtime connect.")

      // check if the connection is outside of container
      if (
        src instanceof OutPort &&
        dst instanceof InPort &&
        src._isContainedBy(this) &&
        dst._isContainedBy(this)
      ) {
        return CanConnectResult.RT_CONNECTION_OUTSIDE_CONTAINER;
      }

      /**
       * TODO (axmmisaka): The following code is commented for multiple reasons:
       * The causality interface check is not fully implemented so new checks are failing
       * Second, direct feedthrough itself would not cause any problem *per se*.
       * To ensure there is no cycle, the safest way is to check against the global dependency graph.
       */

      let app = this as Reactor;
      while (app._getContainer() !== app) {
        app = app._getContainer();
      }
      const graph = app._getPrecedenceGraph();
      graph.addEdge(src, dst);
      if (graph.hasCycle()) {
        return CanConnectResult.RT_CYCLE;
      }

      return CanConnectResult.SUCCESS;
    }
  }

  private _isInScope(src: IOPort<unknown>, dst?: IOPort<unknown>): boolean {
    // Assure that the general scoping and connection rules are adhered to.
    if (src instanceof OutPort) {
      if (dst instanceof InPort) {
        // OUT to IN
        if (
          src._isContainedByContainerOf(this) &&
          dst._isContainedByContainerOf(this)
        ) {
          return true;
        } else {
          return false;
        }
      } else {
        // OUT to OUT
        if (
          src._isContainedByContainerOf(this) &&
          (dst === undefined || dst._isContainedBy(this))
        ) {
          return true;
        } else {
          return false;
        }
      }
    } else {
      if (dst instanceof InPort) {
        // IN to IN
        if (src._isContainedBy(this) && dst._isContainedByContainerOf(this)) {
          return true;
        } else {
          return false;
        }
      } else {
        // IN to OUT
        if (
          src._isContainedBy(this) &&
          dst !== undefined &&
          dst._isContainedBy(this)
        ) {
          return true;
        } else {
          return false;
        }
      }
    }
  }

  /**
   * Connect a source port to a downstream destination port without canConnect() check.
   * This must be used with caution after checking canConnect for the given ports.
   * @param src The source port to connect.
   * @param dst The destination port to connect.
   */
  private _uncheckedConnect<R, S extends R>(
    src: IOPort<S>,
    dst: IOPort<R>
  ): void {
    Log.debug(this, () => `connecting ${src} and ${dst}`);
    // Add dependency implied by connection to local graph.
    this._dependencyGraph.addEdge(src, dst);
    // Register receiver for value propagation.
    const writer = dst.asWritable(this._getKey(dst));
    src
      .getManager(this._getKey(src))
      .addReceiver(writer as unknown as WritablePort<S>);
    const val = src.get();
    if (this._runtime.isRunning() && val !== undefined) {
      writer.set(val);
    }
  }

  /**
   * Connect a source port to a downstream destination port. If a source is a
   * regular port, then the type variable of the source has to be a subtype of
   * the type variable of the destination. If the source is a caller port,
   * then the destination has to be a callee port that is effectively a
   * subtype of the caller port. Because functions have a contra-variant
   * subtype relation, the arguments of the caller side must be a subtype of
   * the callee's, and the return value of the callee's must be a subtype of
   * the caller's.
   * @param src The source port to connect.
   * @param dst The destination port to connect.
   */
  protected _connect<R, S extends R>(src: IOPort<S>, dst: IOPort<R>): void {
    if (src === undefined || src === null) {
      throw new Error("Cannot connect unspecified source");
    }
    if (dst === undefined || dst === null) {
      throw new Error("Cannot connect unspecified destination");
    }
    const canConnectResult = this.canConnect(src, dst);
    // I know, this looks a bit weird. But
    if (canConnectResult !== CanConnectResult.SUCCESS) {
      throw new Error(
        `ERROR connecting ${src} to ${dst}. Reason is ${canConnectResult.valueOf()}`
      );
    }
    this._uncheckedConnect(src, dst);
  }

  protected _connectMulti<R, S extends R>(
    src: Array<MultiPort<S> | IOPort<S>>,
    dest: Array<MultiPort<R> | IOPort<R>>,
    repeatLeft: boolean
  ): void {
    const leftPorts = new Array<IOPort<S>>(0);
    const rightPorts = new Array<IOPort<R>>(0);

    // TODO(hokeun): Check if the multiport's container is Bank when Bank is implemented.
    src.forEach((port) => {
      if (port instanceof MultiPort) {
        port.channels().forEach((singlePort) => {
          leftPorts.push(singlePort);
        });
      } else if (port instanceof IOPort) {
        leftPorts.push(port);
      }
    });

    dest.forEach((port) => {
      if (port instanceof MultiPort) {
        port.channels().forEach((singlePort) => {
          rightPorts.push(singlePort);
        });
      } else if (port instanceof IOPort) {
        rightPorts.push(port);
      }
    });

    if (repeatLeft) {
      const leftPortsSize = leftPorts.length;
      for (let i = 0; leftPorts.length < rightPorts.length; i++) {
        leftPorts.push(leftPorts[i % leftPortsSize]);
      }
    }

    if (leftPorts.length < rightPorts.length) {
      Log.warn(
        null,
        () => "There are more right ports than left ports. ",
        "Not all ports will be connected!"
      );
    } else if (leftPorts.length > rightPorts.length) {
      Log.warn(
        null,
        () => "There are more left ports than right ports. ",
        "Not all ports will be connected!"
      );
    }

    for (let i = 0; i < leftPorts.length && i < rightPorts.length; i++) {
      const canConnectResult = this.canConnect(leftPorts[i], rightPorts[i]);
      if (canConnectResult !== CanConnectResult.SUCCESS) {
        throw new Error(
          `ERROR connecting ${leftPorts[i]} 
                    to ${rightPorts[i]} 
                    in multiple connections from ${src} 
                    to ${dest}`
        );
      }
    }
    for (let i = 0; i < leftPorts.length && i < rightPorts.length; i++) {
      this._uncheckedConnect(leftPorts[i], rightPorts[i]);
    }
  }

  protected _connectCall<A extends T, R, T, S extends R>(
    src: CallerPort<A, R>,
    dst: CalleePort<T, S>
  ): void {
    if (this.canConnectCall(src, dst)) {
      Log.debug(this, () => `connecting ${src} and ${dst}`);
      // Treat connections between callers and callees separately.
      // Note that because A extends T and S extends R, we can safely
      // cast CalleePort<T,S> to CalleePort<A,R>.
      src.remotePort = dst as unknown as CalleePort<A, R>;
      // Register the caller in the callee reactor so that it can
      // establish dependencies on the callers.
      const calleeManager = dst.getManager(this._getKey(dst));
      const callerManager = src.getManager(this._getKey(src));
      const container = callerManager.getContainer();
      const callers = new Set<Reaction<Variable[]>>();
      container._dependencyGraph.getDownstreamNeighbors(src).forEach((dep) => {
        if (dep instanceof Reaction) {
          callers.add(dep);
        }
      });
      const first = container._getFirst(callers);
      const last = container._getLast(callers);
      const lastCaller = calleeManager.getLastCaller();
      if (lastCaller !== undefined) {
        // This means the callee port is bound to a reaction and
        // there may be zero or more callers. We now continue
        // building a chain of callers.
        if (first != null) {
          this._dependencyGraph.addEdge(lastCaller, first);
        } else {
          this._dependencyGraph.addEdge(dst, src);
        }
        if (last != null) calleeManager.setLastCaller(last);
      } else {
        throw new Error("No procedure linked to callee port");
      }
    } else {
      throw new Error(`ERROR connecting ${src} to ${dst}`);
    }
  }

  /**
   * Return a dependency graph consisting of only this reactor's own ports
   * and the dependencies between them.
   */
  protected _getCausalityInterface(): PrecedenceGraph<Port<unknown>> {
    const ifGraph = this._causalityGraph;
    // Find all the input and output ports that this reactor owns.

    const inputs = this._findOwnInputs();
    const outputs = this._findOwnOutputs();
    const visited = new Set();

    const search = (
      output: OutPort<unknown>,
      nodes: Set<Port<unknown> | Reaction<Variable[]>>
    ): void => {
      for (const node of nodes) {
        if (!visited.has(node)) {
          visited.add(node);
          if (node instanceof InPort && inputs.has(node as InPort<unknown>)) {
            ifGraph.addEdge(node, output);
          } else {
            search(output, this._dependencyGraph.getUpstreamNeighbors(output));
          }
        }
      }
    };

    // For each output, walk the graph and add dependencies to
    // the inputs that are reachable.
    for (const output of outputs) {
      search(output, this._dependencyGraph.getUpstreamNeighbors(output));
      visited.clear();
    }

    return ifGraph;
  }

  private _findOwnCalleePorts(): Set<CalleePort<unknown, unknown>> {
    const ports = new Set<CalleePort<unknown, unknown>>();
    for (const component of this._keyChain.keys()) {
      if (component instanceof CalleePort) {
        ports.add(component as CalleePort<unknown, unknown>);
      }
    }
    return ports;
  }

  private _findOwnPorts(): Set<Port<unknown>> {
    const ports = new Set<Port<unknown>>();
    for (const component of this._keyChain.keys()) {
      if (component instanceof Port) {
        ports.add(component as Port<unknown>);
      }
    }
    return ports;
  }

  private _findOwnInputs(): Set<InPort<unknown>> {
    const inputs = new Set<InPort<unknown>>();
    for (const component of this._keyChain.keys()) {
      if (component instanceof InPort) {
        inputs.add(component as InPort<unknown>);
      }
    }
    return inputs;
  }

  private _findOwnOutputs(): Set<OutPort<unknown>> {
    const outputs = new Set<OutPort<unknown>>();
    for (const component of this._keyChain.keys()) {
      if (component instanceof OutPort) {
        outputs.add(component as OutPort<unknown>);
      }
    }
    return outputs;
  }

  private _findOwnReactors(): Set<Reactor> {
    const reactors = new Set<Reactor>();
    for (const component of this._keyChain.keys()) {
      if (component instanceof Reactor) {
        reactors.add(component);
      }
    }
    return reactors;
  }

  /**
   * Delete the connection between the source and destination nodes.
   * If the destination node is not specified, all connections from the source node to any other node are deleted.
   * @param src Source port of connection to be disconnected.
   * @param dst Destination port of connection to be disconnected. If undefined, disconnect all connections from the source port.
   */
  protected _disconnect<R, S extends R>(src: IOPort<S>, dst?: IOPort<R>): void {
    if (
      (!this._runtime.isRunning() && this._isInScope(src, dst)) ||
      this._runtime.isRunning()
    ) {
      this._uncheckedDisconnect(src, dst);
    } else {
      throw new Error(`ERROR disconnecting ${src} to ${dst}`);
    }
  }

  private _uncheckedDisconnect(
    src: IOPort<unknown>,
    dst?: IOPort<unknown>
  ): void {
    Log.debug(this, () => `disconnecting ${src} and ${dst}`);
    if (dst instanceof IOPort) {
      const writer = dst.asWritable(this._getKey(dst));
      src.getManager(this._getKey(src)).delReceiver(writer);
      this._dependencyGraph.removeEdge(src, dst);
    } else {
      const nodes = this._dependencyGraph.getDownstreamNeighbors(src);
      for (const node of nodes) {
        if (node instanceof IOPort) {
          const writer = node.asWritable(this._getKey(node));
          src.getManager(this._getKey(src)).delReceiver(writer);
          this._dependencyGraph.removeEdge(src, node);
        }
      }
    }
  }

  // /**
  //  * Set all the timers of this reactor.
  //  */
  // protected _setTimers(): void {
  //     Log.debug(this, () => "Setting timers for: " + this);
  //     let timers = new Set<Timer>();
  //     for (const [k, v] of Object.entries(this)) {
  //         if (v instanceof Timer) {
  //             this._setTimer(v);
  //         }
  //     }
  // }

  // protected _setTimer(timer: Timer): void {
  //     Log.debug(this, () => ">>>>>>>>>>>>>>>>>>>>>>>>Setting timer: " + timer);
  //     let startTime;
  //     if (timer.offset.isZero()) {
  //         // getLaterTime always returns a microstep of zero, so handle the
  //         // zero offset case explicitly.
  //         startTime = this.util.getCurrentTag().getMicroStepsLater();
  //     } else {
  //         startTime = this.util.getCurrentTag().getLaterTag(timer.offset);
  //     }// FIXME: startup and a timer with offset zero should be simultaneous and not retrigger events
  //     this._schedule(new TaggedEvent(timer, this.util.getCurrentTag().getLaterTag(timer.offset), null));
  // }

  /**
   * Report a timer to the app so that it gets unscheduled.
   * @param timer The timer to report to the app.
   */
  protected _unsetTimer(timer: Timer): void {
    // FIXME: we could either set the timer to 'inactive' to tell the
    // scheduler to ignore future event and prevent it from rescheduling any.
    // The problem with this approach is that if, for some reason, a timer would get
    // reactivated, it could start seeing events that were scheduled prior to its
    // becoming inactive. Alternatively, we could remove the event from the queue,
    // but we'd have to add functionality for this.
  }

  /**
   * Unset all the timers of this reactor.
   */
  protected _unsetTimers(): void {
    // Log.global.debug("Getting timers for: " + this)
    for (const [, v] of Object.entries(this)) {
      if (v instanceof Timer) {
        this._unsetTimer(v);
      }
    }
  }

  /**
   * Return the fully qualified name of this reactor.
   */
  toString(): string {
    return this._getFullyQualifiedName();
  }
}

/*
interface ComponentManager {
    getOwner(): Reactor;
}
*/

/**
 * A caller port sends arguments of type T and receives a response of type R.
 */
export class CallerPort<A, R> extends Port<R> implements Write<A>, Read<R> {
  public get(): R | undefined {
    if (
      this.tag?.isSimultaneousWith(this.runtime.util.getCurrentTag()) ??
      false
    )
      return this.remotePort?.retValue;
  }

  public remotePort: CalleePort<A, R> | undefined;

  public set(value: A): void {
    // Invoke downstream reaction directly, and return store the result.
    if (this.remotePort != null) {
      this.remotePort.invoke(value);
    }
    this.tag = this.runtime.util.getCurrentTag();
  }

  public invoke(value: A): R | undefined {
    // If connected, this will trigger a reaction and update the
    // value of this port.
    this.set(value);
    // Return the updated value.
    return this.get();
  }

  /**
   *
   * @param container Reference to the container of this port
   * (or the container thereof).
   */
  public getManager(key: symbol | undefined): TriggerManager {
    if (this._key === key) {
      return this.manager;
    }
    throw Error("Unable to grant access to manager.");
  }

  protected manager: TriggerManager = new (class implements TriggerManager {
    constructor(private readonly port: CallerPort<A, R>) {}

    addReaction(reaction: Reaction<Variable[]>): void {
      throw new Error("A Caller port cannot use used as a trigger.");
    }

    delReaction(reaction: Reaction<Variable[]>): void {
      throw new Error("A Caller port cannot use used as a trigger.");
    }

    getContainer(): Reactor {
      return this.port._getContainer();
    }
  })(this);

  toString(): string {
    return "CallerPort";
  }
}

interface CalleeManager extends TriggerManager {
  setLastCaller: (reaction: Reaction<Variable[]> | undefined) => void;
  getLastCaller: () => Reaction<Variable[]> | undefined;
  addReaction: (procedure: Procedure<Variable[]>) => void;
  getProcedure: () => Procedure<Variable[]> | undefined;
}

/**
 * A callee port receives arguments of type A and send a response of type R.
 */
export class CalleePort<A, R> extends Port<A> implements Read<A>, Write<R> {
  get(): A | undefined {
    return this.argValue;
  }

  public retValue: R | undefined;

  public argValue: A | undefined;

  protected procedure: Procedure<Variable[]> | undefined;

  protected lastCaller: Reaction<Variable[]> | undefined;

  public invoke(value: A): R | undefined {
    this.argValue = value;
    this.procedure?.doReact();
    return this.retValue;
  }

  public set(value: R): void {
    // NOTE: this will not trigger reactions because
    // connections between caller ports and callee ports
    // are invoked directly.
    this.retValue = value;
  }

  public return(value: R): void {
    this.set(value);
  }

  /**
   *
   * @param key
   */
  public getManager(key: symbol | undefined): CalleeManager {
    if (this._key === key) {
      return this.manager;
    }
    throw Error("Unable to grant access to manager.");
  }

  protected manager: CalleeManager = new (class implements CalleeManager {
    constructor(private readonly port: CalleePort<A, unknown>) {}

    getContainer(): Reactor {
      return this.port._getContainer();
    }

    addReaction(procedure: Reaction<Variable[]>): void {
      if (this.port.procedure !== undefined) {
        throw new Error(
          "Each callee port can trigger only a single" +
            " reaction, but two or more are found on: " +
            this.port.toString()
        );
      }
      this.port.procedure = procedure;
    }

    delReaction(reaction: Reaction<Variable[]>): void {
      throw new Error("Method not implemented.");
    }

    setLastCaller(reaction: Reaction<Variable[]> | undefined): void {
      this.port.lastCaller = reaction;
    }

    getProcedure(): Procedure<Variable[]> | undefined {
      return this.port.procedure;
    }

    getLastCaller(): Reaction<Variable[]> | undefined {
      return this.port.lastCaller;
    }
  })(this);

  toString(): string {
    return "CalleePort";
  }
}

class EventQueue extends PrioritySet<Tag> {
  public push(event: TaggedEvent<unknown>): void {
    super.push(event);
  }

  public pop(): TaggedEvent<unknown> | undefined {
    return super.pop() as TaggedEvent<unknown>;
  }

  public peek(): TaggedEvent<unknown> | undefined {
    return super.peek() as TaggedEvent<unknown>;
  }
}

class ReactionQueue extends PrioritySet<Priority> {
  public push(reaction: Reaction<Variable[]>): void {
    super.push(reaction);
  }

  public pop(): Reaction<Variable[]> {
    return super.pop() as Reaction<Variable[]>;
  }

  public peek(): Reaction<Variable[]> {
    return super.peek() as Reaction<Variable[]>;
  }
}

export interface Runtime {
  util: UtilityFunctions;
  stage: (reaction: Reaction<Variable[]>) => void;
  initialize: (timer: Timer) => void;
  schedule: (e: TaggedEvent<unknown>) => void;
  delete: (r: Reactor) => void;
  isRunning: () => boolean;
}
export interface UtilityFunctions {
  requestStop: () => void;
  reportError: (message?: string) => void;
  requestErrorStop: (message?: string) => void;
  isLastTAGProvisional: () => boolean;
  getCurrentTag: () => Tag;
  getCurrentLogicalTime: () => TimeValue;
  getCurrentPhysicalTime: () => TimeValue;
  getStartTag: () => Tag;
  getStartTime: () => TimeValue;
  getElapsedLogicalTime: () => TimeValue;
  getElapsedPhysicalTime: () => TimeValue;
  sendRTIMessage: <T>(
    data: T,
    destFederateID: number,
    destPortID: number
  ) => void;
  sendRTITimedMessage: <T>(
    data: T,
    destFederateID: number,
    destPortID: number,
    time: TimeValue | undefined
  ) => void;
  sendRTIPortAbsent: (
    destFederateID: number,
    destPortID: number,
    additionalDelay: TimeValue | undefined
  ) => void;
}

export interface MutationSandbox extends ReactionSandbox {
  connect: {
    <R, S extends R>(src: ConnectablePort<S>, dst: ConnectablePort<R>): void;
    <A extends T, R, T, S extends R>(
      src: CallerPort<A, R>,
      dst: CalleePort<T, S>
    ): void;
  };

  disconnect: <R, S extends R>(src: IOPort<S>, dst?: IOPort<R>) => void;

  delete: (reactor: Reactor) => void;

  getReactor: () => Reactor; // Container

  // FIXME:
  // forkJoin(constructor: new () => Reactor, ): void;
}

export interface ReactionSandbox {
  /**
   * Collection of utility functions accessible from within a `react` function.
   */
  util: UtilityFunctions;
  getBankIndex: () => number;
}

export class App extends Reactor {
  readonly _alarm = new Alarm();

  protected _errored = false;

  protected _errorMessage?: string;

  readonly _uuid = uuidv4();

  /**
   * Set of reactions to stage when this app starts executing.
   */
  private readonly _reactionsAtStartup = new Set<Reaction<Variable[]>>();

  /**
   * Set of timers to schedule when this app starts executing.
   */
  private readonly _timersToSchedule = new Set<Timer>();

  /**
   * Set of reactors that gets populated during each execution step,
   * identifying all the terminated reactors that are to be removed
   * at the end of that execution step.
   */
  private readonly _reactorsToRemove = new Array<Reactor>();

  /**
   * Stores whether the current tag's reactions queue is empty.
   * This will be false when a federate waits for network inputs.
   * Note that this should be initialized with false to handle startup
   * reactions of each federate.
   */
  private _isDone = false;

  /**
   * Stores whether the last received TAG (Tag Advance Grant) was provisional.
   * Every federate starts out assuming that it has been granted a PTAG
   * at the start time, or if it has no upstream federates, a TAG.
   */
  protected _isLastTAGProvisional = false;

  /**
   * Inner class that provides access to utilities that are safe to expose to
   * reaction code.
   */
  protected util: UtilityFunctions = new (class implements UtilityFunctions {
    constructor(private readonly app: App) {}

    public requestStop(): void {
      this.app._shutdown();
    }

    public requestErrorStop(message?: string): void {
      this.reportError(message);
      this.app._shutdown();
    }

    public reportError(message?: string): void {
      this.app._errored = true;
      if (this.app._errorMessage === undefined) {
        this.app._errorMessage = message;
      } else {
        this.app._errorMessage += ` || ${message ?? ""}`;
      }
    }

    public isLastTAGProvisional(): boolean {
      return this.app._isLastTAGProvisional;
    }

    public getCurrentTag(): Tag {
      return this.app._currentTag;
    }

    public getCurrentLogicalTime(): TimeValue {
      return this.app._currentTag.time;
    }

    public getCurrentPhysicalTime(): TimeValue {
      return getCurrentPhysicalTime();
    }

    public getStartTag(): Tag {
      return new Tag(this.app._startOfExecution, 0);
    }

    public getStartTime(): TimeValue {
      return this.app._startOfExecution;
    }

    public getElapsedLogicalTime(): TimeValue {
      return this.app._currentTag.time.difference(this.app._startOfExecution);
    }

    public getElapsedPhysicalTime(): TimeValue {
      return getCurrentPhysicalTime().subtract(this.app._startOfExecution);
    }

    public sendRTIMessage<T>(
      data: T,
      destFederateID: number,
      destPortID: number
    ): void {
      this.app.sendRTIMessage(data, destFederateID, destPortID);
    }

    public sendRTITimedMessage<T>(
      data: T,
      destFederateID: number,
      destPortID: number,
      time: TimeValue | undefined
    ): void {
      this.app.sendRTITimedMessage(data, destFederateID, destPortID, time);
    }

    public sendRTIPortAbsent(
      destFederateID: number,
      destPortID: number,
      additionalDelay: TimeValue | undefined
    ): void {
      this.app.sendRTIPortAbsent(destFederateID, destPortID, additionalDelay);
    }
  })(this);

  /**
   * Inner class that provides access to the Runtime object.
   */
  private readonly __runtime: Runtime = new (class implements Runtime {
    util: UtilityFunctions;

    constructor(private readonly app: App) {
      this.util = app.util;
    }

    /**
     * Report whether the runtime has started processing events yet.
     */
    public isRunning(): boolean {
      return this.app._active;
    }

    /**
     * Stage the given reaction for execution at the current logical time.
     * @param reaction The reaction to load onto the reaction queue.
     */
    public stage(reaction: Reaction<Variable[]>): void {
      if (this.app._active) {
        this.app._reactionQ.push(reaction);
      } else {
        // If execution hasn't started yet, collect the staged reactions.
        // They will be queued once they have been assigned priorities.
        this.app._reactionsAtStartup.add(reaction);
      }
    }

    /**
     * Initialize the given timer.
     *
     * If execution has already begun, do the following:
     *  - if the offset is nonzero, schedule the timer at t + offset; and
     *  - otherwise, if the period is nonzero, schedule the it at t + period.
     * If exection is yet to start, postpone initialization until the start
     * time is known.
     *
     * @param timer The timer to initialize.
     */
    public initialize(timer: Timer): void {
      if (this.app._active) {
        Log.debug(
          this,
          () => "Scheduling timer " + timer._getFullyQualifiedName()
        );
        console.log(
          ">>>>>>>>>Scheduling timer " + timer._getFullyQualifiedName()
        );
        // Schedule relative to the current tag.
        let nextTag;
        if (!timer.offset.isZero()) {
          nextTag = this.app._currentTag.getLaterTag(timer.offset);
        } else if (!timer.period.isZero()) {
          nextTag = this.app._currentTag.getLaterTag(timer.period);
        }

        if (nextTag != null) {
          Log.debug(
            this,
            () =>
              "Postponed scheduling of timer " + timer._getFullyQualifiedName()
          );
          this.schedule(new TaggedEvent(timer, nextTag, nextTag));
        }
      } else {
        console.log(
          ">>>>>>>>>Postponed Scheduling of timer " +
            timer._getFullyQualifiedName()
        );
        // If execution hasn't started yet, collect the timers.
        // They will be initialized once it is known what the start time is.
        this.app._timersToSchedule.add(timer);
      }
    }

    /**
     * Push an event onto the event queue.
     * @param e Tagged event to push onto the event queue.
     */
    public schedule(e: TaggedEvent<unknown>): void {
      const head = this.app._eventQ.peek();

      // Don't schedule events past the end of execution.
      if (
        this.app._endOfExecution == null ||
        !this.app._endOfExecution.isSmallerThan(e.tag)
      ) {
        this.app._eventQ.push(e);
      }

      Log.debug(this, () => `Scheduling with trigger: ${e.trigger}`);
      Log.debug(
        this,
        () =>
          `Elapsed logical time in schedule: ${String(
            this.util.getElapsedLogicalTime()
          )}`
      );
      Log.debug(
        this,
        () =>
          `Elapsed physical time in schedule: ${String(
            this.util.getElapsedPhysicalTime()
          )}`
      );

      // If the scheduled event has an earlier tag than whatever is at the
      // head of the queue, set a new alarm.
      if (head === undefined || e.tag.isSmallerThan(head.tag)) {
        this.app._setAlarmOrYield(e.tag);
      }
    }

    /**
     * Mark a reactor for deletion. At the end of logical time at which
     * this method was invoked the reactor will be removed from its
     * container.
     * @param r The reactor to be deleted.
     */
    public delete(r: Reactor): void {
      this.app._reactorsToRemove.push(r);
    }
  })(this);

  /**
   * Send an (untimed) message to the designated federate port through the RTI.
   * This function throws an error if it isn't called on a FederatedApp.
   * @param data The data that contain the body of the message.
   * @param destFederateID The federate ID that is the destination of this message.
   * @param destPortID The port ID that is the destination of this message.
   */
  protected sendRTIMessage<T>(
    data: T,
    destFederateID: number,
    destPortID: number
  ): void {
    throw new Error(
      "Cannot call sendRTIMessage from an App. sendRTIMessage may be called only from a FederatedApp"
    );
  }

  /**
   * Send a (timed) message to the designated federate port through the RTI.
   * This function throws an error if it isn't called on a FederatedApp.
   * @param data The data that contain the body of the message.
   * @param destFederateID The federate ID that is the destination of this message.
   * @param destPortID The port ID that is the destination of this message.
   */
  protected sendRTITimedMessage<T>(
    data: T,
    destFederateID: number,
    destPortID: number,
    time: TimeValue | undefined
  ): void {
    throw new Error(
      "Cannot call sendRTIMessage from an App. sendRTIMessage may be called only from a FederatedApp"
    );
  }

  /**
   * Send a port absent message to the destination port.
   * This function throws an error if it isn't called on a FederatedApp.
   * @param additional_delay The offset applied to the timestamp
   *  using after. The additional delay will be greater or equal to zero
   *  if an after is used on the connection. If no after is given in the
   *  program, NEVER is passed. To be overriden by the FederatedApp class.
   * @param destFederatedID The fed ID of the receiving federate.
   * @param destPortID The ID of the receiving port.
   */
  protected sendRTIPortAbsent(
    destFederateID: number,
    destPortID: number,
    additionalDelay: TimeValue | undefined
  ): void {
    throw new Error(
      "Cannot call sendRTIPortAbsent from an App. sendRTIPortAbsent may be called only from a FederatedApp"
    );
  }

  /**
   * The current time, made available so actions may be scheduled relative to it.
   */
  private _currentTag: Tag;

  /**
   * Reference to "immediate" invocation of next.
   */
  protected _immediateRef: ReturnType<typeof setImmediate> | undefined;

  /**
   * Priority set that keeps track of scheduled events.
   */
  private readonly _eventQ = new EventQueue();

  /**
   * If not null, finish execution with success, this time interval after the
   * start of execution.
   */
  private readonly _executionTimeout: TimeValue | undefined;

  /**
   * The time at which normal execution should terminate. When this time is
   * defined, we can assume that a shutdown event associated with this time
   * has been scheduled.
   */
  private _endOfExecution: Tag | undefined;

  /**
   * If false, execute with normal delays to allow physical time to catch up
   * to logical time. If true, don't wait for physical time to match logical
   * time.
   */
  private readonly _fast: boolean;

  /**
   * Indicates whether the program should continue running once the event
   * queue is empty.
   */
  protected _keepAlive = false;

  /**
   * Priority set that keeps track of reactions at the current Logical time.
   */
  protected readonly _reactionQ = new ReactionQueue();

  /**
   * The physical time when execution began relative to January 1, 1970 00:00:00 UTC.
   * Initialized in start().
   */
  private _startOfExecution!: TimeValue;

  /**
   * Indicates if _finish() was already called.
   * This prevents _finish() from being called recursively.
   */
  private _done = false;

  /**
   * Interval for snoozing and waking up.
   */
  private _advanceMessageInterval: TimeValue = TimeValue.secs(1);

  public setAdvanceMessageInterval(advanceMessageInterval: TimeValue): void {
    this._advanceMessageInterval = advanceMessageInterval;
  }

  /**
   * Unset all the timers of this reactor.
   */
  protected _unsetTimers(): void {
    Object.entries(this)
      .filter((it) => it[1] instanceof Timer)
      .forEach((it) => {
        this._unsetTimer(it[1] as Timer);
      });
  }

  private readonly snooze: Action<Tag>;

  readonly _name: string;

  /**
   * Create a new top-level reactor.
   * @param executionTimeout Optional parameter to let the execution of the app time out.
   * @param keepAlive Optional parameter, if true allows execution to continue with an empty event queue.
   * @param fast Optional parameter, if true does not wait for physical time to catch up to logical time.
   * @param success Optional callback to be used to indicate a successful execution.
   * @param failure Optional callback to be used to indicate a failed execution.
   */
  constructor(
    executionTimeout: TimeValue | undefined = undefined,
    keepAlive = false,
    fast = false,
    public success: () => void = () => undefined,
    public failure: () => void = () => undefined
  ) {
    super(null);

    let name = this.constructor.name;
    if (name === "") {
      name = "app";
    } else {
      name = name.charAt(0).toLowerCase() + name.slice(1);
    }
    this._name = name;

    // Update pointer to runtime object for this reactor and
    // its startup and shutdown action since the inner class
    // instance this.__runtime isn't initialized up until here.
    this._receiveRuntimeObject(this.__runtime);
    this.startup._receiveRuntimeObject(this.__runtime);
    this.shutdown._receiveRuntimeObject(this.__runtime);
    this.__dummy._receiveRuntimeObject(this.__runtime);
    this.snooze = new Action(this, Origin.logical);

    // Initialize the scope in which reactions and mutations of this
    // reactor will execute. This is already done in the super constructor,
    // but has to be redone because at that time this.utils was not
    // initialized at that point.
    this._initializeReactionScope();
    this._initializeMutationScope();

    this._fast = fast;
    this._keepAlive = keepAlive;
    this._executionTimeout = executionTimeout;

    // NOTE: these will be reset properly during startup.
    this._currentTag = new Tag(TimeValue.secs(0), 0);
    this._startOfExecution = this._currentTag.time;
  }

  /**
   * Check whether the next event can be handled or not.
   *
   * In a non-federated context this method always returns true.
   * @param event The next event to be processed.
   */
  protected _canProceed(event: TaggedEvent<unknown>): boolean {
    return true;
  }

  /**
   * Set the current tag to be the next tag.
   *
   * @param event The tag of the next event to be handled.
   */
  protected _advanceTime(nextTag: Tag): void {
    this._currentTag = nextTag;
  }

  protected _iterationComplete(): void {
    return undefined;
  }

  /**
   * Add a dummy event to the event queue.
   * Currently, the sole usage of this is to ensure LET to be sent to the RTI
   * when there is any physical action triggering output port(s) in federated execution.
   *
   * @param tag The tag at which this dummy event occurs.
   */
  protected _addDummyEvent(tag: Tag): void {
    this._eventQ.push(new TaggedEvent(this.__dummy, tag, 0));
  }

  /**
   * Iterate over all reactions in the reaction queue and execute them.
   * @returns Whether every reactions at this tag are executed. This can
   * be false in the federated exection.
   */
  protected _react(): boolean {
    let r: Reaction<Variable[]>;
    while (this._reactionQ.size() > 0) {
      try {
        r = this._reactionQ.pop();
        r.doReact();
      } catch (e) {
        Log.error(this, () => `Exception occurred in reaction: ${r}: ${e}`);
        // Allow errors in reactions to kill execution.
        throw e;
      }
    }
    Log.globalLogger.debug("Finished handling all events at current time.");
    return true;
  }

  /**
   * Do the steps needed for the new logical tag.
   * This function is overriden by federation.ts.
   */
  protected _startTimeStep(): void {
    return undefined;
  }

  /**
   * Pop all events from event_q with timestamp equal to current tag,
   * extract all the reactions triggered by these events, and stick them
   * into the reaction queue.
   */
  private _popEvents(): void {
    // Execute all reactions that are triggered at the current tag
    // in topological order. After that, if the next event on the event queue
    // has the same time (but a greater microstep), repeat. This prevents
    // JS event loop from gaining control and imposing overhead. Asynchronous
    // activity therefore might get blocked, but since the results of such
    // activities are typically reported via physical actions, the tags of
    // the resulting events would be in the future, anyway.
    let nextEvent = this._eventQ.peek();
    do {
      // Keep popping the event queue until the next event has a different tag.
      while (nextEvent?.tag.isSimultaneousWith(this._currentTag) ?? false) {
        const trigger = nextEvent?.trigger;
        this._eventQ.pop();
        Log.debug(this, () => `Popped off the event queue: ${trigger}`);
        // Handle timers.
        if (trigger instanceof Timer) {
          if (!trigger.period.isZero()) {
            Log.debug(this, () => `Rescheduling timer ${trigger}`);

            this.__runtime.schedule(
              new TaggedEvent(
                trigger,
                this._currentTag.getLaterTag(trigger.period),
                null
              )
            );
          }
        }

        // Load reactions onto the reaction queue.
        if (nextEvent != null) {
          trigger?.update(nextEvent);
        }

        // Look at the next event on the queue.
        nextEvent = this._eventQ.peek();
      }

      // // End of this execution step. Perform cleanup.
      // while (this._reactorsToRemove.length > 0) {
      //   // const r = this._reactorsToRemove.pop();
      //   // FIXME: doing this for the entire model at the end of execution
      //   // could be a pretty significant performance hit, so we probably
      //   // don't want to do this
      //   // r?._unplug() FIXME: visibility
      // }

      // Peek at the event queue to see whether we can process the next event
      // or should give control back to the JS event loop.
      nextEvent = this._eventQ.peek();
    } while (
      nextEvent != null &&
      this._currentTag.isSimultaneousWith(nextEvent.tag)
    );
  }

  /**
   * Handle the next events on the event queue.
   * ----
   * Wait until physical time matches or exceeds the time of the least tag on
   * the event queue. After this wait, load the reactions triggered by all
   * events with the least tag onto the reaction queue and start executing
   * reactions in topological order. Each reaction may produce outputs, which,
   * in turn, may load additional reactions onto the reaction queue. Once done
   * executing reactions for the current tag, see if the next tag has the same
   * time (but a different microstep) and repeat the steps above until the
   * next tag has both a different time and microstep. In this case, set an
   * alarm to be woken up at the next time. Note that our timer implementation
   * uses `process.nextTick()` to unravel the stack but prevent I/O from
   * taking place if computation is lagging behind physical time. Only once
   * computation has caught up, full control is given back to the JS event
   * loop. This prevents the system from being overwhelmed with external
   * stimuli.
   */
  private _next(): void {
    let nextEvent = this._eventQ.peek();
    if (nextEvent != null || !this._isDone) {
      if (nextEvent != null && this._isDone) {
        // We're trying to advance a tag to the next event.

        // Check whether the next event can be handled, or not quite yet.
        // A holdup can occur in a federated execution.
        if (!this._canProceed(nextEvent)) {
          // If this happens, then a TAG from the RTI will trigger the
          // next invocation of _next.
          return;
        }
        // If it is too early to handle the next event, set a timer for it
        // (unless the "fast" option is enabled), and give back control to
        // the JS event loop.
        if (
          getCurrentPhysicalTime().isEarlierThan(nextEvent.tag.time) &&
          !this._fast
        ) {
          this._setAlarmOrYield(nextEvent.tag);
          return;
        }

        // Advance logical time.
        this._advanceTime(nextEvent.tag);

        // Start time step.
        this._startTimeStep();
      }
      // Start processing events.
      this._popEvents();

      // React to all the events loaded onto the reaction queue.
      this._isDone = this._react();
      if (!this._isDone) {
        return;
      }
      nextEvent = this._eventQ.peek();

      // Done handling events.
      // _iterationComplete() sends a LTC (Logical Tag Complete) message when federated.
      // Make sure that a federate sends LTC only after actually handling an event.
      this._iterationComplete();
    }

    // Once we've reached here, either we're done processing events and the
    // next event is at a future time, or there are no more events in the
    // queue.
    if (
      this._endOfExecution != null &&
      this._currentTag.isSimultaneousWith(this._endOfExecution)
    ) {
      // An end of execution has been specified; a shutdown event must
      // have been scheduled, and all shutdown events must have been
      // consumed because the next tag is
      this._finish();
    } else {
      if (nextEvent != null) {
        Log.globalLogger.debug("Event queue not empty.");
        this._setAlarmOrYield(nextEvent.tag);
      } else {
        // The queue is empty, and no end of execution has been specified.
        if (this._keepAlive) {
          // Keep alive: snooze and wake up later.
          Log.globalLogger.debug("Going to sleep.");
          this.snooze
            .asSchedulable(this._getKey(this.snooze))
            .schedule(this._advanceMessageInterval, this._currentTag);
        } else {
          // Don't keep alive: initiate shutdown.
          Log.globalLogger.debug("Initiating shutdown.");
          this._shutdown();
        }
      }
    }
  }

  /**
   * Disable the alarm and clear possible immediate next.
   */
  protected _cancelNext(): void {
    this._alarm.unset();
    if (this._immediateRef != null) {
      clearImmediate(this._immediateRef);
      this._immediateRef = undefined;
    }
    this._eventQ.empty();
  }

  /**
   *
   * @param tag
   */
  protected _setAlarmOrYield(tag: Tag): void {
    Log.debug(this, () => {
      return `In setAlarmOrYield for tag: ${tag}`;
    });

    if (this._endOfExecution?.isSmallerThan(tag) ?? false) {
      // Ignore this request if the tag is later than the end of execution.
      return;
    }

    const physicalTime = getCurrentPhysicalTime();
    const timeout = physicalTime.difference(tag.time);
    if (physicalTime.isEarlierThan(tag.time) && !this._fast) {
      // Set an alarm to be woken up when the event's tag matches physical
      // time.
      this._alarm.set(
        function (this: App) {
          this._next();
        }.bind(this),
        timeout
      );
    } else {
      // Either we're in "fast" mode, or we're lagging behind.
      this._requestImmediateInvocationOfNext();
    }
  }

  /**
   * Request an immediate invocation of `this._next()`.
   */
  protected _requestImmediateInvocationOfNext(): void {
    // Only schedule an immediate if none is already pending.
    if (this._immediateRef == null) {
      this._immediateRef = setImmediate(
        function (this: App) {
          this._immediateRef = undefined;
          this._next();
        }.bind(this)
      );
    }
  }

  /**
   * Schedule a shutdown event at the current time if no such action has been taken yet.
   * Clear the alarm, and set the end of execution to be the current tag.
   */
  protected _shutdown(): void {
    if (
      this.__runtime.isRunning() &&
      (this._endOfExecution === undefined ||
        this._currentTag.time.isEarlierThan(this._endOfExecution.time))
    ) {
      this._endOfExecution = this._currentTag.getMicroStepsLater(1); // FIXME: this could be a longer delay in distributed execution

      Log.debug(this, () => "Stop requested.");
      Log.debug(
        this,
        () => `Setting end of execution to: ${this._endOfExecution}`
      );

      this.schedulable(this.shutdown).schedule(0, null);
    } else {
      Log.globalLogger.debug(
        "Ignoring App._shutdown() call after shutdown has already started."
      );
    }
  }

  /**
   * Setter for _endOfExecution to be used used by the subclass, FederatedApp.
   */
  protected _setEndOfExecution(stopTag: Tag): void {
    this._endOfExecution = stopTag;
    this.__runtime.schedule(
      new TaggedEvent(this.shutdown, this._endOfExecution, null)
    );
  }

  /**
   * Getter for _endOfExecution to be used used by the subclass, FederatedApp.
   */
  protected _getEndOfExecution(): Tag | undefined {
    return this._endOfExecution;
  }

  /**
   * Wrap up execution by logging information and reporting errors if applicable.
   */
  protected _finish(): void {
    if (this._done) {
      return;
    }
    this._done = true;
    this._cancelNext();
    Log.info(this, () => Log.hr);
    Log.info(
      this,
      () =>
        `>>> End of execution at (logical) time: ${String(
          this.util.getCurrentLogicalTime()
        )}`
    );
    Log.info(
      this,
      () =>
        `>>> Elapsed physical time: ${String(
          this.util.getElapsedPhysicalTime()
        )}`
    );
    Log.info(this, () => Log.hr);

    if (this._errored) {
      console.error(">>> Erroneous exit.");
      if (this._errorMessage != null) {
        console.error(`Reason: ${this._errorMessage}`);
      }
      this.failure();
    } else {
      this.success();
    }
  }

  /**
   * Analyze the dependencies between reactions in this app.
   *
   * Assign priorities that encode the precedence relations between
   * reactions. If there exist circular dependencies, throw an exception.
   * This method should only be invoked prior to the start of execution,
   * never during execution.
   */
  protected _analyzeDependencies(): void {
    Log.info(this, () => Log.hr);
    const initStart = getCurrentPhysicalTime();
    Log.globalLogger.info(">>> Initializing");

    Log.globalLogger.debug("Initiating startup sequence.");

    // Obtain the precedence graph, ensure it has no cycles,
    // and assign a priority to each reaction in the graph.
    const apg = this._getPrecedenceGraph();

    console.log(apg.toString());

    Log.debug(this, () => "Before collapse: " + apg.toString());

    // 1. Collapse dependencies and weed out the ports.
    const collapsed = new ReactionGraph(apg);

    // 2. Update priorities.
    Log.debug(this, () => "After collapse: " + collapsed.toString());

    if (collapsed.updatePriorities(true)) {
      Log.globalLogger.debug("No cycles.");
    } else {
      throw new Error("Cycle in reaction graph.");
    }

    Log.info(
      this,
      () =>
        `>>> Spent ${String(
          getCurrentPhysicalTime().subtract(initStart)
        )} checking the precedence graph.`
    );
  }

  /**
   * Set the logical start time of the execution according to the given time
   * value. If an execution timeout is defined, also determine the end of
   * execution is as the start time plus the execution timeout. This method
   * also marks the app as "active" and initializes any timers that may have
   * been created during the course of this app's instantiation.
   * @param startTime The beginning of this app's execution. The end of
   * execution is determined relative to this TimeValue is a timeout has
   * been set.
   */
  protected _determineStartAndEndOfExecution(startTime: TimeValue): void {
    // Let the start of the execution be the current physical time.
    this._startOfExecution = startTime;
    this._currentTag = new Tag(this._startOfExecution, 0);

    // Mark the app as active, now that the start time is known.
    this._active = true;

    // Schedule all timers created during the instantiation of this app.
    this._timersToSchedule.forEach((timer) => {
      this.__runtime.initialize(timer);
    });

    if (this._executionTimeout != null) {
      this._endOfExecution = new Tag(
        this._startOfExecution.add(this._executionTimeout),
        0
      );
      Log.debug(this, () => `Execution timeout: ${this._executionTimeout}`);

      // If there is a known end of execution, schedule a shutdown reaction to that effect.
      this.__runtime.schedule(
        new TaggedEvent(this.shutdown, this._endOfExecution, null)
      );
    }
  }

  /**
   * Load all reactions that were staged for immediate execution during this
   * app's instantiation onto the reaction queue.
   */
  protected _loadStartupReactions(): void {
    this._reactionsAtStartup.forEach((r) => {
      this._reactionQ.push(r);
    });
  }

  /**
   * Start executing reactions.
   */
  protected _startExecuting(): void {
    Log.info(this, () => Log.hr);
    Log.info(this, () => Log.hr);

    Log.info(this, () => `>>> Start of execution: ${this._currentTag}`);
    Log.info(this, () => Log.hr);

    // Handle the reactions that were loaded onto the reaction queue.
    this._isDone = this._react();

    // Continue execution by processing the next event.
    this._next();
  }

  /**
   * Start the app.
   */
  public _start(): void {
    // First analyze the dependency graph to determine whether it is valid.
    this._analyzeDependencies();

    // Then load any reactions that were staged during the instantiation of
    // any of the reactors.
    this._loadStartupReactions();

    // Use the current physical time to set the app's start of execution.
    this._determineStartAndEndOfExecution(getCurrentPhysicalTime());

    // Start the main event loop.
    this._startExecuting();
  }
}
