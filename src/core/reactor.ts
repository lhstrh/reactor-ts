/**
 * Core of the reactor runtime.
 * 
 * @author Marten Lohstroh (marten@berkeley.edu),
 * @author Matt Weber (matt.weber@berkeley.edu)
 */

import {PrioritySetElement, PrioritySet, SortableDependencyGraph, Log, DependencyGraph} from './util';
import {TimeValue, TimeUnit, Tag, Origin, getCurrentPhysicalTime, UnitBasedTimeValue, Alarm} from './time';
import {Component} from "./component"
import {Reaction, Priority, Mutation, Procedure} from "./reaction"

// Set the default log level.
Log.global.level = Log.levels.ERROR;

//--------------------------------------------------------------------------//
// Types                                                                    //
//--------------------------------------------------------------------------//

/**
 * Type that denotes the absence of a value.
 * @see Variable
 */
export type Absent = undefined;

/**
 * Conditional type for argument lists of reactions. If the type variable
 * `T` is inferred to be a subtype of `Variable[]` it will yield `T`; it  
 * will yield `never` if `T` is not a subtype of `Variable[]`.
 * @see Reaction
 */
export type ArgList<T> = T extends Variable[] ? T : never;

/**
 * Type for data exchanged between ports.
 */
export type Present = (number | string | boolean | symbol | object | null);

/**
 * Type for simple variables that are both readable and writable.
 */
export type ReadWrite<T> = Read<T> & Write<T>;

/**
 * A variable can be read, written to, or scheduled. Variables may be passed to
 * reactions in an argument list.
 * @see Read
 * @see Write
 * @see Schedule
 */
export type Variable = Read<unknown>

//--------------------------------------------------------------------------//
// Constants                                                                //
//--------------------------------------------------------------------------//

const defaultMIT = new UnitBasedTimeValue(1, TimeUnit.nsec); // FIXME

//--------------------------------------------------------------------------//
// Interfaces                                                               //
//--------------------------------------------------------------------------//

/**
 * Interface for the invocation of remote procedures.
 */
export interface Call<A, R> extends Write<A>, Read<R> {
    invoke(args: A): R | undefined;
}

/**
 * Interface for readable variables.
 */
export interface Read<T> {
    get(): T | Absent;
}

/**
 * Interface for schedulable actions.
 */
export interface Schedule<T> extends Read<T> {
    schedule: (extraDelay: TimeValue | 0, value: T) => void;
    // FIXME: it makes sense to be able to check the presence of a (re)schedulable action.
}

/**
 * Interface for writable ports.
 */
export interface Write<T> {
    set: (value: T) => void;
}

/**
 * Abstract class for a writable port. It is intended as a wrapper for a
 * regular port. In addition to a get method, it also has a set method and
 * a method for retrieving the port that it wraps.
 */
export abstract class WritablePort<T extends Present> implements ReadWrite<T> {
    abstract get(): T | undefined;
    abstract set(value: T): void;
    abstract getPort(): IOPort<T>
}

/**
 * Abstract class for a schedulable action. It is intended as a wrapper for a
 * regular action. In addition to a get method, it also has a schedule method
 * that allows for the action to be scheduled.
 */
export abstract class SchedulableAction<T extends Present> implements Schedule<T> {
    abstract get(): T | undefined;
    abstract schedule(extraDelay: 0 | TimeValue, value: T): void;
}

//--------------------------------------------------------------------------//
// Core Reactor Classes                                                     //
//--------------------------------------------------------------------------//

/**
 * An event is caused by a timer or a scheduled action. Each event is tagged
 * with a time instant and may carry a value of arbitrary type. The tag will
 * determine the event's position with respect to other events in the event
 * queue.
 */
export class TaggedEvent<T extends Present> implements PrioritySetElement<Tag> {

    /**
     * Pointer to the next element of the priority set that this event might
     * be hooked into.
     */
    public next: PrioritySetElement<Tag> | undefined;

    /**
     * Construct a new tagged event.
     * @param trigger The trigger of this event.
     * @param tag The tag at which this event occurs.
     * @param value The value associated with this event. 
     * 
     */
    constructor(public trigger: ScheduledTrigger<T>, public tag: Tag, public value: T) {
    }

    /**
     * Return true if this event has a smaller tag than the given event, false
     * otherwise.
     * @param node The event to compare this event's tag against.
     */
    hasPriorityOver(node: PrioritySetElement<Tag> | undefined) {
        if (node) {
            return this.getPriority().isSmallerThan(node.getPriority());
        } else {
            return false;
        }
    }

    /**
     * Determine whether the given event is a duplicate of this one. If so, assign the
     * value this event to the given one. Otherwise, return false.
     * @param node The event adopt the value from if it is a duplicate of this one.
     */
    updateIfDuplicateOf(node: PrioritySetElement<Tag> | undefined) {
        if (node && node instanceof TaggedEvent) {
            if (this.trigger === node.trigger && this.tag.isSimultaneousWith(node.tag)) {
                node.value = this.value; // update the value
                return true;
            }
        }
        return false;
    }

    /**
     * Return the tag associated with this event.
     */
    getPriority(): Tag {
        return this.tag;
    }
}
/**
 * Abstract class for a trigger. A trigger may be an action, port, or timer.
 */
abstract class Trigger extends Component {

    /**
     * Reactions to trigger.
     */
    protected reactions: Set<Reaction<unknown>> = new Set();
    
    /**
     * Request the manager of this trigger. The request will only be honored
     * if the correct key is given. Each component has a unique symbol (a key)
     * that is handed to the owner upon instantiation of the component. If the
     * wrong key is supplied, return undefined.
     * @param key The private key embedded in this trigger.
     */
    abstract getManager(key: Symbol | undefined): TriggerManager;

    /**
     * Return the owner of this trigger.
     */
    public getContainer(): Reactor | null {
        return this._getContainer()
    }

    /**
     * Return whether or not this trigger is present.
     */
    abstract isPresent(): boolean;

}

/**
 * 
 */
abstract class ScheduledTrigger<T extends Present> extends Trigger {
    protected value: T | Absent = undefined;
    protected tag: Tag | undefined;
    
    protected runtime!: Runtime;

    constructor(container: Reactor, alias?: string) { // FIXME: do we really want the alias here? Probably not.
        super(container, alias)
        this._linkToRuntimeObject()
    }

    /**
     * Update the current value of this timer in accordance with the given
     * event, and trigger any reactions that list this timer as their trigger.
     * @param e Timestamped event.
     */
    public update(e: TaggedEvent<T>):void {

        if (!e.tag.isSimultaneousWith(this.runtime.util.getCurrentTag())) {
            throw new Error("Time of event does not match current logical time.");
        }
        if (e.trigger === this) {
            this.value = e.value
            this.tag = e.tag;
            for (let r of this.reactions) {
                this.runtime.stage(r)
            }
        } else {
            throw new Error("Attempt to update action using incompatible event.");
        }
    }

    public getManager(key: Symbol | undefined): TriggerManager {
        if (this._key == key) {
            return this.manager
        }
        throw Error("Unable to grant access to manager.")
    }

    /**
     * Returns true if this action was scheduled for the current
     * logical time. This result is not affected by whether it
     * has a value.
     */
    public isPresent() {
        if (this.tag === undefined) {
            // This action has never been scheduled before.
            return false;
        }
        if (this.tag.isSimultaneousWith(this.runtime.util.getCurrentTag())) {
            return true;
        } else {
            return false;
        }
    }

    protected manager = new class implements TriggerManager {
        constructor(private trigger: ScheduledTrigger<T>) { }
        getContainer(): Reactor {
            return this.trigger._getContainer()
        }
        addReaction(reaction: Reaction<unknown>): void {
            this.trigger.reactions.add(reaction)
        }
        delReaction(reaction: Reaction<unknown>): void {
            this.trigger.reactions.delete(reaction)
        }
    }(this)

    public _receiveRuntimeObject(runtime: Runtime) {
        if (!this.runtime) {
            this.runtime = runtime
        } else {
            throw new Error("Can only establish link to runtime once.")
        }
    }

}

/**
 * An action denotes a self-scheduled event.
 * An action, like an input, can cause reactions to be invoked.
 * Whereas inputs are provided by other reactors, actions are scheduled
 * by this reactor itself, either in response to some observed external
 * event or as a delayed response to some input event. The action can be
 * scheduled by a reactor by invoking the schedule function in a reaction
 * or in an asynchronous callback that has been set up in a reaction.
 */
export class Action<T extends Present> extends ScheduledTrigger<T> implements Read<T> {

    readonly origin: Origin;
    readonly minDelay: TimeValue;
    readonly minInterArrival: TimeValue = defaultMIT;
    
    public get(): T | Absent {
        if (this.isPresent()) {
            return this.value;
        } else {
            return undefined;
        }
    }

    public asSchedulable(key: Symbol | undefined): Schedule<T> {
        if (this._key === key) {
            return this.scheduler
        }
        throw Error("Invalid reference to container.")
    }

    public getManager(key: Symbol | undefined): TriggerManager {
        if (this._key == key) {
            return this.manager
        }
        throw Error("Unable to grant access to manager.")
    }

    protected scheduler = new class<T extends Present> extends SchedulableAction<T> {
        get(): T | undefined {
            return this.action.get()
        }
        constructor(private action: Action<T>) {
            super()
        }
        schedule(extraDelay: 0 | TimeValue, value: T): void {
            if (!(extraDelay instanceof TimeValue)) {
                extraDelay = new TimeValue(0);
            }
            
            var tag = this.action.runtime.util.getCurrentTag();
            var delay = this.action.minDelay.add(extraDelay);

            tag = tag.getLaterTag(delay);

            if (this.action.origin == Origin.physical) {
                // If the resulting timestamp from delay is less than the current physical time
                // on the platform, then the timestamp becomes the current physical time.
                // Otherwise the tag is computed like a logical action's tag.

                let physicalTime = getCurrentPhysicalTime();
                if (tag.time.isEarlierThan(physicalTime)) {
                    tag = new Tag(getCurrentPhysicalTime(), 0);
                } else {
                    tag = tag.getMicroStepLater();
                }
            }
    
            if (this.action.origin == Origin.logical && !(this.action instanceof Startup)) {
                tag = tag.getMicroStepLater();
            }
            
            Log.debug(this, () => "Scheduling " + this.action.origin +
                " action " + this.action._getFullyQualifiedName() + " with tag: " + tag);
    
            this.action.runtime.schedule(new TaggedEvent(this.action, tag, value));
        }
    }(this)

    /** 
     * Construct a new action.
     * @param __container__ The reactor containing this action.
     * @param origin Optional. If physical, then the hardware clock on the local 
     * platform is used to determine the tag of the resulting event. If logical, 
     * the current logical time (plus one microstep) is used as the offset.
     * @param minDelay Optional. Defaults to 0. Specifies the intrinsic delay of
     * any events resulting from scheduling this action.
     * @param minInterArrival Optional. Defaults to 1 nsec. Specifies the minimum
     * intrinsic delay between to occurrences of this action.
     */
    constructor(__container__: Reactor, origin: Origin, minDelay: TimeValue = new TimeValue(0), minInterArrival: TimeValue = defaultMIT) {
        super(__container__);
        this.origin = origin;
        this.minDelay = minDelay;
    }

    public toString() {
        return this._getFullyQualifiedName();
    }
}

export class Startup extends Action<Present> { // FIXME: this should not be a schedulable trigger, just a trigger
    constructor(__parent__: Reactor) {
        super(__parent__, Origin.logical)
    }
}

export class Shutdown extends Action<Present> {
    constructor(__parent__: Reactor) {
        super(__parent__, Origin.logical)
    }
}

export class Parameter<T> implements Read<T> {
    constructor(private value: T) {
    }
    get(): T {
        return this.value;
    }
}

/**
 * A state variable. This class refines the Read interface by letting `get`
 * return T rather than T | Absent. If the state should be nullable or
 * uninitialized, this has to be reflected explicitly in T.
 */
export class State<T> implements Read<T>, Write<T> {
    
    /**
     * Create a new state variable and assign it an initial value.
     * @param value The initial value to assign to this state variable.
     */
    constructor(private value: T) {
    
    }

    /**
     * Return the current value of this state variable.
     */
    get(): T {
        return this.value;
    };

    /**
     * Set the current value of this state variable.
     * @param value 
     */
    set(value: T) {
        this.value = value;
    };

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
    constructor(__container__: Reactor, offset: TimeValue | 0, period: TimeValue | 0) {
        super(__container__);
        
        if (!(offset instanceof TimeValue)) {
            this.offset = new TimeValue(0);
        } else {
            this.offset = offset;
        }

        if (!(period instanceof TimeValue)) {
            this.period = new TimeValue(0);
        } else {
            this.period = period;
        }
        Log.debug(this, () => "Creating timer: " + this._getFullyQualifiedName())
        // Initialize this timer.
        this.runtime.initialize(this)
    }


    public toString() {
        return "Timer from " + this._getContainer()._getFullyQualifiedName() + " with period: " + this.period + " offset: " + this.offset;
    }

    public get(): Tag | Absent {
        if (this.isPresent()) {
            return this.tag;
        } else {
            return undefined;
        }
    }

}

export class Args<T extends Variable[]> {
    tuple: T;
    constructor(...args: T) {
        this.tuple = args;
    }
}

export class Triggers {
    list: Variable[];
    constructor(trigger: Variable, ...triggers: Variable[]) {
        this.list = triggers.concat(trigger)
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
     * Data structure to keep track of register components.
     * Note: declare this class member before any other ones as they may
     * attempt to access it.
     */
    private _keyChain: Map<Component, Symbol> = new Map()

    /**
     * Collection of priviledged functions that are passed down from the
     * container.
     */
    private _runtime!: Runtime;

    /**
     * This graph has in it all the dependencies implied by this reactor's
     * ports, reactions, and connections.
     */
    private _dependencyGraph: DependencyGraph<Port<Present> | Reaction<unknown>> = new DependencyGraph()

    /**
     * This graph has some overlap with the reactors dependency, but is 
     * different in two respects:
     * - transitive dependencies between ports have been collapsed; and
     * - it incorporates the causality interfaces of all contained reactors.
     * It thereby carries enough information to find out whether adding a new
     * connection at runtime could result in a cyclic dependency, _without_ 
     * having to consult other reactors.
     */
    private _causalityGraph: DependencyGraph<Port<Present>> = new DependencyGraph()
    
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
     * The list of reactions this reactor has.
     */
    private _reactions: Reaction<any>[] = [];

    /**
     * Sandbox for the execution of reactions.
     */
    private _reactionScope!: ReactionSandbox;

    /** 
     * The list of mutations this reactor has.
     */
    private _mutations: Mutation<any>[] = [];

    /**
     * Sandbox for the execution of mutations.
     */
    private _mutationScope!: MutationSandbox;

    /**
     * Receive the runtime object from the container of this reactor.
     * Invoking this method in any user-written code will result in a
     * runtime error.
     * @param runtime The runtime object handed down from the container.
     */
    public _receiveRuntimeObject(runtime: Runtime) {
        if (!this._runtime && runtime) {
            this._runtime = runtime
            // In addition to setting the runtime object, also make its
            // utility functions available as a protected member.
            this.util = runtime.util
        } else {
            throw new Error("Can only establish link to runtime once.")
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
    public _register(component: Component, key: Symbol) {
        if (component._isRegistered()) {
            throw new Error("Unable to register " 
            + component._getFullyQualifiedName() 
            + " as it already has a container.")
        }
        // Only add key if the component isn't a self-reference
        // and isn't already registered.
        if (component !== this && !this._keyChain.has(component)) {
            this._keyChain.set(component, key)
        }
    }

    public _requestRuntimeObject(component: Component): void {
        if (component._isContainedBy(this)) {
            component._receiveRuntimeObject(this._runtime)
        }
    }

    /**
     * Remove all the connections associated with a given reactor.
     * @param reactor 
     */
    private _deleteConnections(reactor: Reactor): void {
        for (let port of reactor._findOwnPorts()) {
            this._dependencyGraph.removeNode(port)
        }
    }

    /**
     * Remove this reactor from its container and sever any connections it may
     * still have. This reactor will become defunct and is ready for garbage
     * collection.
     */
    protected _unplug() {
        this._getContainer()._deregister(this, this._key)
    }

    /**
     * Remove the given reactor and its connections from this container if
     * the key matches.
     * @param reactor
     * @param key 
     */
    public _deregister(reactor: Reactor, key: Symbol) {
        let found
        for (let v of this._keyChain.values()) {
            if (v === key) {
                found = true
                break
            }
        }
        if (found) {
            this._keyChain.delete(reactor)
            this._deleteConnections(reactor)
        } else {
            console.log("Unable to deregister reactor: " + reactor._getFullyQualifiedName())
        }
    }


    private _getLast(reactions: Set<Reaction<any>>): Reaction<unknown> | undefined {
        let index = -1
        let all = this._getReactionsAndMutations()

        for (let reaction of reactions) {
            let found = all.findIndex((r) => r === reaction)
            if (found >= 0) {
                index = Math.max(found, index)
            }
        }
        if (index >= 0) {
            return all[index]
        }
    }

    private _getFirst(reactions: Set<Reaction<any>>): Reaction<unknown> | undefined {
        let index = -1
        let all = this._getReactionsAndMutations()

        for (let reaction of reactions) {
            let found = all.findIndex((r) => r === reaction)
            if (found >= 0) {
                index = Math.min(found, index)
            }
        }
        if (index >= 0) {
            return all[index]
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
    protected _getKey(component: Trigger, key?: Symbol): Symbol | undefined {
        if (component._isContainedBy(this) || this._key === key) {
            return this._keyChain.get(component)
        } else if (!(component instanceof Action) && 
                    component._isContainedByContainerOf(this)) {
            let owner = component.getContainer()
            if (owner !== null) {
                return owner._getKey(component, this._keyChain.get(owner))
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
    private _delete() {
        //console.log("Marking for deletion: " + this._getFullyQualifiedName())
        this._runtime.delete(this)
        this.shutdown.update(new TaggedEvent(this.shutdown, this.util.getCurrentTag(), null))
        this._findOwnReactors().forEach(r => r._delete())
    }

    /**
     * Inner class intended to provide access to methods that should be
     * accessible to mutations, not to reactions.
     */
    private _MutationSandbox = class implements MutationSandbox { 
        constructor(private reactor: Reactor) {}
        
        /**
         * Basic utilities.
         */
        public util = this.reactor.util
        
        /**
         * 
         * @param src 
         * @param dst 
         */
        public connect<A extends T, R extends Present, T extends Present, S extends R>
                (src: CallerPort<A,R> | IOPort<S>, dst: CalleePort<T,S> | IOPort<R>) {
            return this.reactor._connect(src, dst);
        }

        /**
         * Return the reactor containing the mutation using this sandbox.
         */
        public getReactor(): Reactor {
            return this.reactor
        }

        /**
         * Mark the given reactor for deletion.
         * 
         * @param reactor 
         */
        public delete(reactor: Reactor) {
            reactor._delete()
        }
    };
    
    /**
     * Inner class that furnishes an execution environment for reactions.  
     */
    private _ReactionSandbox = class implements ReactionSandbox {
        public util: UtilityFunctions;
        constructor(public reactor: Reactor) {
            this.util = reactor.util
        }
    }

    /**
     * Create a new reactor.
     * @param container The container of this reactor.
     */

    constructor(container: Reactor | null, alias?:string) {
        super(container, alias);
        
        this._linkToRuntimeObject()
        
        this.shutdown = new Shutdown(this);
        this.startup = new Startup(this);

        // Utils get passed down the hierarchy. If this is an App,
        // the container refers to this object, making the following
        // assignment idemponent.
        //this.util = this._getContainer().util
        
        // Add a default mutation to the top-level reactor that deletes all
        // of its contained reactors when it is triggered by shutdown.
        if (this instanceof App) {
            // Pass in a reference to the reactor because the runtime object
            // is inaccessible since it is created after this code runs.
            let self = this
            this.addMutation(new Triggers(this.shutdown), new Args(), function(this) {
                console.log(">>>>>>> Top-level shutdown mutation <<<<<<<")
                self._findOwnReactors().forEach(r => r._delete())
            })
        } else {
            // Create sandboxes for the reactions and mutations to execute in.
            this._reactionScope = new this._ReactionSandbox(this)
            this._mutationScope = new this._MutationSandbox(this)
        }
    }

    protected _initializeReactionScope(): void {
        this._reactionScope = new this._ReactionSandbox(this)
    }

    protected _initializeMutationScope(): void {
        this._mutationScope = new this._MutationSandbox(this)
    }
    
    // protected _isActive(): boolean {
    //     return this._active
    // }

    protected writable<T extends Present>(port: IOPort<T>): ReadWrite<T> {
        return port.asWritable(this._getKey(port));
    }

    /**
     * Return the index of the reaction given as an argument.
     * @param reaction The reaction to return the index of.
     */
    public _getReactionIndex(reaction: Reaction<any>): number {
        
        var index: number | undefined;

        if (reaction instanceof Mutation) {
            index = this._mutations.indexOf(reaction)
        } else {
            index = this._reactions.indexOf(reaction)
        }
        
        if (index !== undefined)
            return index

        throw new Error("Reaction is not listed.");
    }

    protected schedulable<T extends Present>(action: Action<T>): Schedule<T> {
        return action.asSchedulable(this._getKey(action));
    }

    private _recordDeps<T extends Variable[]>(reaction: Reaction<any>) {
        
        // Add a dependency on the previous reaction or mutation, if it exists.
        let prev = this._getLastReactionOrMutation()
        if (prev) {
            this._dependencyGraph.addEdge(reaction, prev)
        }

        // FIXME: Add a dependency on the last mutation that the owner of this reactor
        // has. How do we know that it is the last? We have a "lastCaller" problem here.
        // Probably better to solve this at the level of the dependency graph with a function
        // that allows for a link to be updated.

        // Set up the triggers.
        for (let t of reaction.trigs.list) {
            // Link the trigger to the reaction.
            if (t instanceof Trigger) {
                t.getManager(this._getKey(t)).addReaction(reaction)
            }

            // Also record this trigger as a dependency.
            if (t instanceof IOPort) {
                this._dependencyGraph.addEdge(reaction, t)
                //this._addDependency(t, reaction);
            } else {
                Log.debug(this, () => ">>>>>>>> not a dependency: " + t);
            }
        }
        
        let sources = new Set<Port<any>>()
        let effects = new Set<Port<any>>()
    
        for (let a of reaction.args.tuple) {
            if (a instanceof IOPort) {
                this._dependencyGraph.addEdge(reaction, a)
                sources.add(a)
            }
            if (a instanceof CalleePort) {
                this._dependencyGraph.addEdge(a, reaction)
            }
            if (a instanceof CallerPort) {
                this._dependencyGraph.addEdge(reaction, a)
            }
            // Only necessary if we want to add actions to the dependency graph.
            if (a instanceof Action) {
                // dep
            }
            if (a instanceof SchedulableAction) {
                // antidep
            }
            if (a instanceof WritablePort) {
                this._dependencyGraph.addEdge(a.getPort(), reaction)
                effects.add(a.getPort())
            }
        }
        // Make effects dependent on sources.
        for (let effect of effects) {
            this._causalityGraph.addEdges(effect, sources)
        }
    }

    /**
     * Given a reaction, return the reaction within this reactor that directly
     * precedes it, or `undefined` if there is none.
     * @param reaction A reaction to find the predecessor of. 
     */
    protected prevReaction(reaction: Reaction<unknown>): Reaction<any> | undefined {
        var index: number | undefined
        
        if (reaction instanceof Mutation) {
            index = this._mutations.indexOf(reaction)
            if (index !== undefined && index > 0) {
                return this._mutations[index-1];
            }
        } else {
            index = this._reactions.indexOf(reaction)
            if (index !== undefined && index > 0) {
                return this._reactions[index-1];
            } else {
                let len = this._mutations.length
                if (len > 0) {
                    return this._mutations[len-1]
                }
            }
        }
    }

    /**
     * Given a reaction, return the reaction within this reactior that directly
     * succeeds it, or `undefined` if there is none.
     * @param reaction A reaction to find the successor of. 
     */
    protected nextReaction(reaction: Reaction<unknown>): Reaction<any> | undefined {
        var index: number | undefined
        
        if (reaction instanceof Mutation) {
            index = this._mutations.indexOf(reaction)
            if (index !== undefined && index < this._mutations.length-1) {
                return this._mutations[index+1];
            } else if (this._reactions.length > 0) {
                return this._reactions[0]
            }
        } else {
            index = this._reactions.indexOf(reaction)
            if (index !== undefined && index < this._reactions.length-1) {
                return this._reactions[index+1];
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
    protected addReaction<T>(trigs: Triggers, args: Args<ArgList<T>>,
        react: (this: ReactionSandbox, ...args: ArgList<T>) => void, deadline?: TimeValue,
        late: (this: ReactionSandbox, ...args: ArgList<T>) => void =
            () => { Log.global.warn("Deadline violation occurred!") }) {
        let calleePorts = trigs.list.filter(trig => trig instanceof CalleePort)
        
        if (calleePorts.length > 0) {
            // This is a procedure.
            let port = calleePorts[0] as CalleePort<Present, Present>
            let procedure = new Procedure(this, this._reactionScope, trigs, args, react, deadline, late)
            if (trigs.list.length > 1) {
                // A procedure can only have a single trigger.
                throw new Error("Procedure `" + procedure + "` has multiple triggers.")
            }
            procedure.active = true
            this._recordDeps(procedure);
            // Let the last caller point to the reaction that precedes this one.
            // This lets the first caller depend on it.
            port.getManager(this._getKey(port)).setLastCaller(this._getLastReactionOrMutation())
            this._reactions.push(procedure);    
            
        } else {
            // This is an ordinary reaction.
            let reaction = new Reaction(this, this._reactionScope, trigs, args, react, deadline, late);
            // Stage it directly if it to be triggered immediately.
            if (reaction.isTriggeredImmediately()) {
                if (this._runtime === undefined) {
                    throw new Error("undefined exec tools in " + this._getFullyQualifiedName())
                }
                this._runtime.stage(reaction as Reaction<unknown>)
                // FIXME: if we're already running, then we need to set the priority as well.
            }
            reaction.active = true;
            this._recordDeps(reaction);
            this._reactions.push(reaction);
        }
    }

    protected addMutation<T>(trigs: Triggers, args: Args<ArgList<T>>,
        react: (this: MutationSandbox, ...args: ArgList<T>) => void, deadline?: TimeValue,
        late: (this: MutationSandbox, ...args: ArgList<T>) => void =
            () => { Log.global.warn("Deadline violation occurred!") }) {
        let mutation = new Mutation(this, this._mutationScope, trigs, args, react,  deadline, late);
        // Stage it directly if it to be triggered immediately.
        if (mutation.isTriggeredImmediately()) {
            this._runtime.stage(mutation as unknown as Reaction<unknown>) // FIXME: types
        }
        mutation.active = true
        this._recordDeps(mutation);
        this._mutations.push(mutation);
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
     * @param depth The depth of recursion.
     */
    protected _getPrecedenceGraph(depth=-1): DependencyGraph<Port<Present> | Reaction<unknown>> {
        
        var graph: DependencyGraph<Port<Present> | Reaction<unknown>> = new DependencyGraph();
        
        graph.merge(this._dependencyGraph)

        if (depth > 0 || depth < 0) {
            if (depth > 0) {
                depth--
            }
            for (let r of this._getOwnReactors()) {
                graph.merge(r._getPrecedenceGraph(depth));
            }
        }
        
        // FIXME: Potentially do this in connect instead upon connecting to a
        // callee port. So far, it is unclear how RPCs would work when
        // established at runtime by a mutation.
        //  
        // Check if there are any callee ports owned by this reactor.
        // If there are, add a dependency from its last caller to the antidependencies
        // of the procedure (excluding the callee port itself).
        let calleePorts = this._findOwnCalleePorts()
        for (let p of calleePorts) {
            let procedure = p.getManager(this._getKey(p)).getProcedure()
            let lastCaller = p.getManager(this._getKey(p)).getLastCaller()
            if (procedure && lastCaller) {
                let effects = graph.getBackEdges(procedure)
                //console.log(">>>>>>>>>>>> last caller:" + lastCaller)
                for (let e of effects) {
                    if (!(e instanceof CalleePort)) {
                        // Add edge to returned graph.
                        graph.addEdge(e, lastCaller)
                        // Also add edge to the local graph.
                        this._dependencyGraph.addEdge(e, lastCaller)
                    }
                }
            } else {
                Error("No procedure")
            }
        }

        return graph;

    }
    
    /**
     * Return the reactors that this reactor owns.
     */
    private _getOwnReactors(): Array<Reactor> {
        return Array.from(this._keyChain.keys()).filter(
            (it) => it instanceof Reactor) as Array<Reactor>;
    }

    /**
     * Return a list of reactions owned by this reactor.
     */
    protected _getReactions(): Array<Reaction<unknown>> {
        var arr: Array<Reaction<any>> = new Array();
        this._reactions.forEach((it) => arr.push(it))
        return arr;
    }

    /**
     * Return a list of reactions and mutations owned by this reactor.
     */
    protected _getReactionsAndMutations(): Array<Reaction<unknown>> {
        var arr: Array<Reaction<any>> = new Array();
        this._mutations.forEach((it) => arr.push(it))
        this._reactions.forEach((it) => arr.push(it))
        return arr;
    }

    protected _getLastReactionOrMutation(): Reaction<any> | undefined {
        let len = this._reactions.length
        if (len > 0) {
            return this._reactions[len -1]
        }
        len = this._mutations.length
        if (len > 0) {
            return this._mutations[len -1]
        }
    }

    /**
     * Return a list of reactions owned by this reactor.
     */
    protected _getMutations(): Array<Reaction<unknown>> {
        var arr: Array<Reaction<any>> = new Array();
        this._mutations.forEach((it) => arr.push(it))
        return arr;
    }

    /**
     * Report whether the given port is downstream of this reactor. If so, the
     * given port can be connected to with an output port of this reactor.
     * @param port 
     */
    public _isDownstream(port: Port<Present>) {
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
    public _isUpstream(port: Port<Present>) {
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
    public canConnect<A extends T, R extends Present, T extends Present, S extends R>
            (src: CallerPort<A,R> | IOPort<S>, dst: CalleePort<T,S> | IOPort<R>) {
        // Immediate rule out trivial self loops. 
        if (src === dst) {
            return false
        }

        if (this._runtime.isRunning() == false) {
            console.log("Connecting before running")
            // Validate connections between callers and callees.
            if (src instanceof CalleePort) {
                return false
            }
            if (src instanceof CallerPort) {
                if (dst instanceof CalleePort && 
                    src._isContainedByContainerOf(this) && dst._isContainedByContainerOf(this)) {
                    return true
                }
                return false
            }
            // Additional checks for regular ports.
            if (dst instanceof IOPort) {
                console.log("IOPort")
                // Rule out write conflicts.
                //   - (between reactors)
                if (!(dst instanceof CalleePort) &&
                    this._dependencyGraph.getBackEdges(dst).size > 0) {
                    return false;
                }

                //   - between reactors and reactions (NOTE: check also needs to happen
                //     in addReaction)
                var deps = this._dependencyGraph.getEdges(dst) // FIXME this will change with multiplex ports
                if (deps != undefined && deps.size > 0) {
                    return false;
                }

                return this._isInScope(src, dst)
            }
        } else {
            // Attempt to make a connection while executing.
            // Check the local dependency graph to figure out whether this change
            // introduces zero-delay feedback.
            // console.log("Runtime connect.")
            // Take the local graph and merge in all the causality interfaces
            // of contained reactors. Then:
            let graph: DependencyGraph<Port<Present> | Reaction<unknown>> = new DependencyGraph()
            graph.merge(this._dependencyGraph)

            for (let r of this._getOwnReactors()) {
                graph.merge(r._getCausalityInterface())
            }
            
            // Add the new edge.
            graph.addEdge(dst, src)

            // 1) check for loops
            if (graph.hasCycle()) {
                return false
            }

            // 2) check for direct feed through.
            let inputs = this._findOwnInputs()
            for (let output of this._findOwnOutputs()) {
                let newReachable = graph.reachableOrigins(output, inputs)
                let oldReachable = this._causalityGraph.reachableOrigins(output, inputs)

                for (let origin of newReachable) {
                    if (origin instanceof Port && !oldReachable.has(origin)) {
                        return false
                    }
                }
            }
            return true
        }
    }

    private _isInScope(src: IOPort<Present>, dst: IOPort<Present>): boolean {
        // Assure that the general scoping and connection rules are adhered to.
        if (src instanceof OutPort) {
            if (dst instanceof InPort) {
                // OUT to IN
                if (src._isContainedByContainerOf(this) && dst._isContainedByContainerOf(this)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                // OUT to OUT
                if (src._isContainedByContainerOf(this) && dst._isContainedBy(this)) {
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
                return false;
            }
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
    protected _connect<A extends T, R extends Present, T extends Present, S extends R>
            (src: CallerPort<A,R> | IOPort<S>, dst: CalleePort<T,S> | IOPort<R>) {
        if (this.canConnect(src, dst)) {
            Log.debug(this, () => "connecting " + src + " and " + dst);
            if (src instanceof CallerPort && dst instanceof CalleePort) {
                // Treat connections between callers and callees separately.
                // Note that because A extends T and S extends R, we can safely
                // cast CalleePort<T,S> to CalleePort<A,R>.
                src.remotePort = ((dst as unknown) as CalleePort<A,R>);
                // Register the caller in the callee reactor so that it can
                // establish dependencies on the callers.
                let calleeManager = dst.getManager(this._getKey(dst))
                let callerManager = src.getManager(this._getKey(src))
                let container = callerManager.getContainer()
                let callers = new Set<Reaction<any>>()
                container._dependencyGraph.getBackEdges(src).forEach((dep) => {
                    if (dep instanceof Reaction) {
                        callers.add(dep)
                    }
                })
                let first = container._getFirst(callers)
                let last = container._getLast(callers)
                let lastCaller = calleeManager.getLastCaller()
                if (lastCaller !== undefined) {
                    // This means the callee port is bound to a reaction and
                    // there may be zero or more callers. We now continue
                    // building a chain of callers.
                    if (first) {
                        this._dependencyGraph.addEdge(first, lastCaller)
                    } else {
                        this._dependencyGraph.addEdge(src, dst)
                    }
                    if (last)
                        calleeManager.setLastCaller(last)
                } else {
                    throw new Error("No procedure linked to callee"
                    + " port `${procedure}`.")
                }
                
            } else if (src instanceof IOPort && dst instanceof IOPort) {
                Log.debug(this, () => "connecting " + src + " and " + dst);
                // Add dependency implied by connection to local graph.
                this._dependencyGraph.addEdge(dst, src);
                // Register receiver for value propagation.
                src.getManager(this._getKey(src)).addReceiver
                    (dst.asWritable(this._getKey(dst)) as WritablePort<S>);
            }
        } else {
            throw new Error("ERROR connecting " + src + " to " + dst);
        }
    }

    /**
     * Return a dependency graph consisting of only this reactor's own ports
     * and the dependencies between them.
     */
    protected _getCausalityInterface(): DependencyGraph<Port<Present>> {
        let ifGraph = this._causalityGraph
        // Find all the input and output ports that this reactor owns.
        
        let inputs = this._findOwnInputs()
        let outputs = this._findOwnOutputs()
        let visited = new Set()
        let self = this
        
        function search(output: OutPort<Present>, nodes: Set<Port<Present> | Reaction<unknown>>) {
            for (let node of nodes) {
                if (!visited.has(node)) {
                    visited.add(node)
                    if (node instanceof InPort && inputs.has(node)) {
                        ifGraph.addEdge(output, node)   
                    } else {
                        search(output, self._dependencyGraph.getEdges(output))
                    }
                }
            }
        }

        // For each output, walk the graph and add dependencies to 
        // the inputs that are reachable.
        for (let output of outputs) {
            search(output, this._dependencyGraph.getEdges(output))
            visited.clear()
        }
        
        return ifGraph
    }

    private _findOwnCalleePorts() {
        let ports = new Set<CalleePort<Present, Present>>()
        for(let component of this._keyChain.keys()) {
            if (component instanceof CalleePort) {
                ports.add(component)
            }
        }
        return ports
    }

    private _findOwnPorts() {
        let ports = new Set<Port<Present>>()
        for (let component of this._keyChain.keys()) {
            if (component instanceof Port) {
                ports.add(component)
            }
        }
        return ports
    }

    private _findOwnInputs() {
        let inputs = new Set<InPort<Present>>()
        for (let component of this._keyChain.keys()) {
            if (component instanceof InPort) {
                inputs.add(component)
            }
        }
        return inputs
    }

    private _findOwnOutputs() {
        let outputs = new Set<OutPort<Present>>()
        for (let component of this._keyChain.keys()) {
            if (component instanceof InPort) {
                outputs.add(component)
            }
        }
        return outputs
    }

    protected _findOwnReactors() {
        let reactors = new Set<Reactor>()
        for(let component of this._keyChain.keys()) {
            if (component instanceof Reactor) {
                reactors.add(component)
            }
        }
        return reactors
    }


    /**
     * 
     * @param src 
     * @param dst 
     */
    private _disconnect(src: Port<Present>, dst: Port<Present>) {
        Log.debug(this, () => "disconnecting " + src + " and " + dst);
        //src.getManager(this.getKey(src)).delReceiver(dst);


        // FIXME

        // let dests = this._destinationPorts.get(src);
        // if (dests != null) {
        //     dests.delete(dst);
        // }
        // this._sourcePort.delete(src);
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
    //         startTime = this.util.getCurrentTag().getMicroStepLater();
    //     } else {
    //         startTime = this.util.getCurrentTag().getLaterTag(timer.offset);
    //     }// FIXME: startup and a timer with offset zero should be simultaneous and not retrigger events
    //     this._schedule(new TaggedEvent(timer, this.util.getCurrentTag().getLaterTag(timer.offset), null));
    // }

    /**
     * Report a timer to the app so that it gets unscheduled.
     * @param timer The timer to report to the app.
     */
    protected _unsetTimer(timer: Timer) {
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
        let timers = new Set<Timer>();
        for (const [k, v] of Object.entries(this)) {
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

export abstract class Port<T extends Present> extends Trigger implements Read<T> {
    
    runtime!: Runtime;

    constructor(container: Reactor, alias?: string) {
        super(container, alias)
        this._linkToRuntimeObject()
    }

    /** The time stamp associated with this port's value. */
    protected tag: Tag | undefined;

    /** The value associated with this port. */
    protected value: T | Absent;

    abstract get(): T | undefined;

    public _receiveRuntimeObject(runtime: Runtime) {
        if (!this.runtime) {
            this.runtime = runtime
        } else {
            throw new Error("Can only establish link to runtime once. Name: " + this._getFullyQualifiedName())
        }
    }

    /**
     * Returns true if the connected port's value has been set; false otherwise
     */
    public isPresent() {

        Log.debug(this, () => "In isPresent()...")
        Log.debug(this, () => "value: " + this.value);
        Log.debug(this, () => "tag: " + this.tag);
        Log.debug(this, () => "time: " + this.runtime.util.getCurrentLogicalTime())

        if (this.value !== undefined
            && this.tag !== undefined
            && this.tag.isSimultaneousWith(this.runtime.util.getCurrentTag())) {
            return true;
        } else {
            return false;
        }
    }
}

export abstract class IOPort<T extends Present> extends Port<T> {

    protected receivers: Set<WritablePort<T>> = new Set();

    /**
     * Return the value set to this port. Return `Absent` if the connected
     * output did not have its value set at the current logical time.
     */
    public get(): T | Absent {
        if (this.isPresent()) {
            return this.value;
        } else {
            return undefined;
        }
    }

    /**
     * Only the holder of the key may obtain a writable port.
     * @param key
     */
    public asWritable(key: Symbol | undefined): WritablePort<T> {
        if (this._key === key) {
            return this.writer
        }
        throw Error("Referenced port is out of scope: " + this._getFullyQualifiedName()) // FIXME: adjust messages for other methods as well
        // FIXME: we could potentially do this for reads/triggers as well just for scope rule enforcement
    }

    /**
     * 
     * @param container Reference to the container of this port 
     * (or the container thereof).
     */
    public getManager(key: Symbol | undefined): IOPortManager<T> {
        if (this._key == key) {
            return this.manager
        }
        throw Error("Unable to grant access to manager.")
    }

    /**
     * Inner class instance to gain access to Write<T> interface.
     */
    protected writer = new class extends WritablePort<T> {
        constructor(private port:IOPort<T>) {
            super()
        }

        public set(value: T): void {
            this.port.value = value;
            this.port.tag = this.port.runtime.util.getCurrentTag();
            // Set values in downstream receivers.
            this.port.receivers.forEach(p => p.set(value))
            // Stage triggered reactions for execution.
            this.port.reactions.forEach(r => this.port.runtime.stage(r))
        }

        public get(): T | Absent {
            return this.port.get()
        }

        public getPort(): IOPort<T> {
            return this.port
        }
        
        public toString(): string {
            return this.port.toString()
        }
        
    }(this)

    /**
     * Inner class instance to let the container configure this port.
     */
    protected manager:IOPortManager<T> = new class implements IOPortManager<T> {
        constructor(private port:IOPort<T>) {}
        getContainer(): Reactor {
            return this.port._getContainer()
        }
        addReceiver(port: WritablePort<T>): void {
            this.port.receivers.add(port)
        }
        delReceiver(port: WritablePort<T>): void {
            this.port.receivers.delete(port)
        }
        addReaction(reaction: Reaction<unknown>): void {
            this.port.reactions.add(reaction)
        }
        delReaction(reaction: Reaction<unknown>): void {
            this.port.reactions.delete(reaction)
        }
    }(this)

    toString(): string {
        return this._getFullyQualifiedName();
    }
}

interface ComponentManager {
    getOwner(): Reactor;

}

interface TriggerManager {
    getContainer():Reactor;
    addReaction(reaction: Reaction<unknown>): void;
    delReaction(reaction: Reaction<unknown>): void;    
}

interface IOPortManager<T extends Present> extends TriggerManager {
    addReceiver(port: WritablePort<T>): void;
    delReceiver(port: WritablePort<T>): void;
}

export class OutPort<T extends Present> extends IOPort<T> {

}

export class InPort<T extends Present> extends IOPort<T> {

}

/**
 * A caller port sends arguments of type T and receives a response of type R.
 */
export class CallerPort<A extends Present, R extends Present> extends Port<R> implements Write<A>, Read<R> {
    
    public get(): R | undefined {
        if (this.tag?.isSimultaneousWith(this.runtime.util.getCurrentTag()))
            return this.remotePort?.retValue
    }

    public remotePort: CalleePort<A, R> | undefined;

    public set(value: A): void  {
        // Invoke downstream reaction directly, and return store the result.
        if (this.remotePort) {
            this.remotePort.invoke(value)
        }
        this.tag = this.runtime.util.getCurrentTag();
    }

    public invoke(value:A): R | undefined {
        // If connected, this will trigger a reaction and update the 
        // value of this port.
        this.set(value)
        // Return the updated value.
        return this.get()
    }

    /**
     * 
     * @param container Reference to the container of this port 
     * (or the container thereof).
     */
    public getManager(key: Symbol | undefined): TriggerManager {
        if (this._key == key) {
            return this.manager
        }
        throw Error("Unable to grant access to manager.")
    }


    protected manager: TriggerManager = new class implements TriggerManager {
        constructor(private port:CallerPort<A, R>) {}
        addReaction(reaction: Reaction<unknown>): void {
            throw new Error("A Caller port cannot use used as a trigger.");
        }
        delReaction(reaction: Reaction<unknown>): void {
            throw new Error("A Caller port cannot use used as a trigger.");
        }
        getContainer(): Reactor {
            return this.port._getContainer()
        }
    }(this)

    toString() {
        return "CallerPort"
    }

}

interface CalleeManager<T extends Present> extends TriggerManager {
    setLastCaller(reaction: Reaction<unknown> | undefined):void;
    getLastCaller(): Reaction<unknown> | undefined;
    addReaction(procedure: Procedure<unknown>): void;
    getProcedure(): Procedure<unknown> | undefined;
}

/**
 * A callee port receives arguments of type A and send a response of type R.
 */
export class CalleePort<A extends Present, R extends Present> extends Port<A> implements Read<A>, Write<R> {
    
    get(): A | undefined {
        return this.argValue;
    }

    public retValue: R | undefined;

    public argValue: A | undefined;

    private procedure: Procedure<unknown> | undefined

    private lastCaller: Reaction<unknown> | undefined
    
    public invoke(value: A): R | undefined {
        this.argValue = value
        this.procedure?.doReact()
        return this.retValue
    }

    public set(value: R): void  {
        // NOTE: this will not trigger reactions because
        // connections between caller ports and callee ports
        // are invoked directly.
        this.retValue = value;
    }

    public return(value: R): void {
        this.set(value)
    }

    /**
     * 
     * @param key 
     */
    public getManager(key: Symbol | undefined): CalleeManager<A> {
        if (this._key == key) {
            return this.manager
        }
        throw Error("Unable to grant access to manager.")
    }

    protected manager:CalleeManager<A> = new class implements CalleeManager<A> {
        constructor(private port:CalleePort<A, Present>) {}
        getContainer(): Reactor {
            return this.port._getContainer()
        }
        addReaction(procedure: Reaction<unknown>): void {
            if (this.port.procedure !== undefined) {
                throw new Error("Each callee port can trigger only a single"
                + " reaction, but two or more are found on: " 
                + this.port.toString())
            }
            this.port.procedure = procedure
        }
        delReaction(reaction: Reaction<unknown>): void {
            throw new Error("Method not implemented.");
        }
        setLastCaller(reaction: Reaction<unknown> | undefined):void {
            this.port.lastCaller = reaction
        }
        getProcedure(): Procedure<unknown> | undefined {
            return this.port.procedure
        }
        getLastCaller(): Reaction<unknown> | undefined {
            return this.port.lastCaller
        }
    }(this)

    toString() {
        return "CalleePort"
    }

}

class EventQueue extends PrioritySet<Tag> {

    public push(event: TaggedEvent<Present>) {
        return super.push(event);
    }

    public pop(): TaggedEvent<Present> | undefined {
        return super.pop() as TaggedEvent<Present>;
    }

    public peek(): TaggedEvent<Present> | undefined {
        return super.peek() as TaggedEvent<Present>;
    }
}

class ReactionQueue extends PrioritySet<Priority> {

    public push(reaction: Reaction<unknown>) {
        return super.push(reaction);
    }

    public pop(): Reaction<unknown> {
        return super.pop() as Reaction<unknown>;
    }

    public peek(): Reaction<unknown> {
        return super.peek() as Reaction<unknown>;
    }

}

export interface Runtime {
    util:UtilityFunctions;
    //core: CoreFunctions;
    stage(reaction: Reaction<unknown>): void;
    initialize(timer: Timer): void;
    schedule(e: TaggedEvent<any>): void;
    delete(r: Reactor): void;
    isRunning(): boolean;
}

// interface CoreFunctions {
//     stage(reaction: Reaction<unknown>): void;
//     initialize(timer: Timer): void;
//     schedule(e: TaggedEvent<any>): void;
//     mark(r: Reactor): void;
//     isRunning(): boolean;
// }

interface UtilityFunctions { // 
    requestShutdown(success: boolean, message?: string): void;
    getCurrentTag(): Tag;
    getCurrentLogicalTime(): TimeValue;
    getCurrentPhysicalTime(): TimeValue;
    getElapsedLogicalTime(): TimeValue;
    getElapsedPhysicalTime(): TimeValue;
    sendRTIMessage(data: Buffer, destFederateID: number, destPortID: number): void;
    sendRTITimedMessage(data: Buffer, destFederateID: number, destPortID: number): void;
}

export interface MutationSandbox extends ReactionSandbox {
    connect<A extends T, R extends Present, T extends Present, S extends R>
            (src: CallerPort<A,R> | IOPort<S>, dst: CalleePort<T,S> | IOPort<R>):void;

    //disconnect(src: Port<Present>, dst?: Port<Present>): void;

    delete(reactor: Reactor): void;

    getReactor(): Reactor; // Container

    // FIXME:    
    //forkJoin(constructor: new () => Reactor, ): void;

}

export interface ReactionSandbox {
    /**
     * Collection of utility functions accessible from within a `react` function.
     */
    util: UtilityFunctions

}

export class App extends Reactor {

    readonly _alarm = new Alarm();

    private _errored = false
    private _message?: string

    /**
     * Set of reactions to stage when this app starts executing.
     */
    private _reactionsAtStartup = new Set<Reaction<unknown>>();

    /**
     * Set of timers to schedule when this app starts executing.
     */
    private _timersToSchedule = new Set<Timer>();

    /**
     * Set of reactors that gets populated during each execution step,
     * identifying all the terminated reactors that are to be removed
     * at the end of that execution step.
     */
    private _reactorsToRemove = new Array<Reactor>();

    /**
     * Inner class that provides access to utilities that are safe to expose to
     * reaction code.
     */
    protected util: UtilityFunctions = new class implements UtilityFunctions {
        constructor(private app: App) {

        }

        public requestShutdown(success: boolean, message?: string) {
            if (!success) {
                this.app._errored = true
            }
            this.app._message = message
            this.app._shutdown()
        }

        public success() {
            return this.app.success();
        }

        public failure() {
            return this.app.failure();
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

        public getElapsedLogicalTime(): TimeValue {
            return this.app._currentTag.time.difference(this.app._startOfExecution);
        }

        public getElapsedPhysicalTime(): TimeValue {
            return getCurrentPhysicalTime().subtract(this.app._startOfExecution);
        }
        
        public sendRTIMessage(data: Buffer, destFederateID: number, destPortID: number) {
            return this.app.sendRTIMessage(data, destFederateID, destPortID);
        };

        public sendRTITimedMessage(data: Buffer, destFederateID: number, destPortID: number) {
            return this.app.sendRTITimedMessage(data, destFederateID, destPortID);
        };

    }(this);


    /**
     * 
     */
    private __runtime: Runtime = new class implements Runtime {
        util: UtilityFunctions        

        constructor(private app: App) {
            this.util = app.util
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
        public stage(reaction: Reaction<unknown>): void {
            if (this.app._active) {
                this.app._reactionQ.push(reaction)
            } else {
                // If execution hasn't started yet, collect the staged reactions.
                // They will be queued once they have been assigned priorities.
                this.app._reactionsAtStartup.add(reaction) 
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
                Log.debug(this, () => "Scheduling timer " + timer._getFullyQualifiedName())
                console.log(">>>>>>>>>Scheduling timer " + timer._getFullyQualifiedName())
                // Schedule relative to the current tag.
                var nextTag;
                if (!timer.offset.isZero()) {
                    nextTag = this.app._currentTag.getLaterTag(timer.offset)
                } else if (!timer.period.isZero()) {
                    nextTag = this.app._currentTag.getLaterTag(timer.period)
                }
                
                if (nextTag) {
                    Log.debug(this, () => "Postponed scheduling of timer " + timer._getFullyQualifiedName())
                    this.schedule(new TaggedEvent(timer, nextTag, nextTag))
                }
    
            } else {
                console.log(">>>>>>>>>Postponed Scheduling of timer " + timer._getFullyQualifiedName())
                // If execution hasn't started yet, collect the timers.
                // They will be initialized once it is known what the start time is.
                this.app._timersToSchedule.add(timer) 
            }
        }
        /**
         * Push an event onto the event queue. 
         * @param e Tagged event to push onto the event queue.
         */
        public schedule(e: TaggedEvent<any>) {
            let head = this.app._eventQ.peek();

            // Don't schedule events past the end of execution.
            if (!this.app._endOfExecution || !this.app._endOfExecution.isSmallerThan(e.tag)) {
                this.app._eventQ.push(e);
            }
            
            Log.debug(this, () => "Scheduling with trigger: " + e.trigger);
            Log.debug(this, () => "Elapsed logical time in schedule: " + this.util.getElapsedLogicalTime());
            Log.debug(this, () => "Elapsed physical time in schedule: " + this.util.getElapsedPhysicalTime());
            
            // If the scheduled event has an earlier tag than whatever is at the
            // head of the queue, set a new alarm.
            if (head == undefined || e.tag.isSmallerThan(head.tag)) {
                this.app._setAlarmOrYield(e.tag);
            }
        }

        /**
         * Mark a reactor for deletion. At the end of logical time at which
         * this method was invoked the reactor will be removed from its
         * container.
         * @param r 
         */
        public delete(r: Reactor): void {
            this.app._reactorsToRemove.push(r)
        };


    }(this)

    /**
     * Send an (untimed) message to the designated federate port through the RTI.
     * This function throws an error if it isn't called on a FederatedApp.
     * @param data A Buffer containing the body of the message.
     * @param destFederateID The federate ID that is the destination of this message.
     * @param destPortID The port ID that is the destination of this message.
     */
    protected sendRTIMessage(data: Buffer, destFederateID: number, destPortID: number) {
        throw new Error("Cannot call sendRTIMessage from an App. sendRTIMessage may be called only from a FederatedApp");
    }

    /**
     * Send a (timed) message to the designated federate port through the RTI.
     * This function throws an error if it isn't called on a FederatedApp.
     * @param data A Buffer containing the body of the message.
     * @param destFederateID The federate ID that is the destination of this message.
     * @param destPortID The port ID that is the destination of this message.
     */
    protected sendRTITimedMessage(data: Buffer, destFederateID: number, destPortID: number) {
        throw new Error("Cannot call sendRTIMessage from an App. sendRTIMessage may be called only from a FederatedApp");
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
    private _eventQ = new EventQueue();

    /**
     * If not null, finish execution with success, this time interval after the
     * start of execution.
     */
    private _executionTimeout: TimeValue | undefined;

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
    private _fast: boolean;

    /**
     * Indicates whether the program should continue running once the event 
     * queue is empty.
     */
    private _keepAlive = false;

    /**
     * Priority set that keeps track of reactions at the current Logical time.
     */
    private _reactionQ = new ReactionQueue();

    /**
     * The physical time when execution began relative to January 1, 1970 00:00:00 UTC.
     * Initialized in start().
     */
    private _startOfExecution!: TimeValue;

    /**
     * Unset all the timers of this reactor.
     */
    protected _unsetTimers(): void {
        Object.entries(this).filter(it => it[1] instanceof Timer).forEach(it => this._unsetTimer(it[1]))
    }

    private snooze: Action<Tag>;

    /**
     * Create a new top-level reactor.
     * @param executionTimeout Optional parameter to let the execution of the app time out.
     * @param keepAlive Optional parameter, if true allows execution to continue with an empty event queue.
     * @param fast Optional parameter, if true does not wait for physical time to catch up to logical time.
     * @param success Optional callback to be used to indicate a successful execution.
     * @param failure Optional callback to be used to indicate a failed execution.
     */
    constructor(executionTimeout: TimeValue | undefined = undefined, 
                keepAlive: boolean = false, 
                fast: boolean = false, 
                public success: () => void = () => {}, 
                public failure: () => void = () => { 
                    throw new Error("An unexpected error has occurred.") 
                }) {
        super(null);
        
        // Update pointer to runtime object for this reactor and
        // its startup and shutdown action since the inner class
        // instance this.__runtime isn't initialized up until here.
        this._receiveRuntimeObject(this.__runtime)
        this.startup._receiveRuntimeObject(this.__runtime)
        this.shutdown._receiveRuntimeObject(this.__runtime)
        this.snooze = new Action(this, Origin.logical, new TimeValue(1, 0))

        // Initialize the scope in which reactions and mutations of this
        // reactor will execute. This is already done in the super constructor,
        // but has to be redone because at that time this.utils was not
        // initialized at that point.
        this._initializeReactionScope()
        this._initializeMutationScope()

        this._fast = fast;
        this._keepAlive = keepAlive;
        this._executionTimeout = executionTimeout;

        // NOTE: these will be reset properly during startup.
        this._currentTag = new Tag(new TimeValue(0), 0);
        this._startOfExecution = this._currentTag.time;
    }

    /**
     * Check whether the next event can be handled or not.
     *
     * In a non-federated context this method always returns true.
     * @param event The next event to be processed.
     */
    protected _canProceed(event: TaggedEvent<Present>) {
        return true
    }

    /**
     * Set the current tag to be the next tag.
     * 
     * @param event The tag of the next event to be handled.
     */
    protected _advanceTime(nextTag: Tag) {
        this._currentTag = nextTag;
    }

    /**
     * Iterate over all reactions in the reaction queue and execute them.
     */
    private _react() {
        while (this._reactionQ.size() > 0) {
            try {
                var r = this._reactionQ.pop();
                r.doReact();
            } catch (e) {
                Log.error(this, () => "Exception occurred in reaction: " + r + ": " + e);
                // Allow errors in reactions to kill execution.
                throw e; 
            }            
        }
        Log.global.debug("Finished handling all events at current time.");
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
    private _next() {
        var nextEvent = this._eventQ.peek();
        if (nextEvent) {

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
            if (getCurrentPhysicalTime().isEarlierThan(nextEvent.tag.time)
                        && !this._fast) {
                this._setAlarmOrYield(nextEvent.tag);
                return;
            }

            // Start processing events. Execute all reactions that are triggered
            // at the current tag in topological order. After that, if the next
            // event on the event queue has the same time (but a greater
            // microstep), repeat. This prevents JS event loop from gaining
            // control and imposing overhead. Asynchronous activity therefore
            // might get blocked, but since the results of such activities are
            // typically reported via physical actions, the tags of the
            // resulting events would be in the future, anyway.
            do {
                // Advance logical time.
                this._advanceTime(nextEvent.tag)

                // Keep popping the event queue until the next event has a different tag.
                while (nextEvent != null && nextEvent.tag.isSimultaneousWith(this._currentTag)) {
                    var trigger = nextEvent.trigger;
                    this._eventQ.pop();
                    Log.debug(this, () => "Popped off the event queue: " + trigger);
                    // Handle timers.
                    if (trigger instanceof Timer) {
                        if (!trigger.period.isZero()) {
                            Log.debug(this, () => "Rescheduling timer " + trigger);

                            this.__runtime.schedule(new TaggedEvent(trigger,
                                this._currentTag.getLaterTag(trigger.period),
                                null));
                        }
                    }

                    // Load reactions onto the reaction queue.
                    trigger.update(nextEvent);
                    
                    // Look at the next event on the queue.
                    nextEvent = this._eventQ.peek();
                }

                // React to all the events loaded onto the reaction queue.
                this._react()
                
                // End of this execution step. Perform cleanup.
                while (this._reactorsToRemove.length > 0) {
                    let r = this._reactorsToRemove.pop()
                    // FIXME: doing this for the entire model at the end of execution
                    // could be a pretty significant performance hit, so we probably
                    // don't want to do this
                    // r?._unplug() FIXME: visibility
                }

                // Peek at the event queue to see whether we can process the next event
                // or should give control back to the JS event loop.
                nextEvent = this._eventQ.peek();

            } while (nextEvent && this._currentTag.time.isEqualTo(nextEvent.tag.time));
        }

        // Once we've reached here, either we're done processing events and the
        // next event is at a future time, or there are no more events in the
        // queue.        
        if (this._endOfExecution && this._currentTag.isSimultaneousWith(this._endOfExecution)) {
            // An end of execution has been specified; a shutdown event must
            // have been scheduled, and all shutdown events must have been
            // consumed because the next tag is 
            this._done();
        } else {
            if (nextEvent) {
                Log.global.debug("Event queue not empty.")
                this._setAlarmOrYield(nextEvent.tag);
            } else {
                // The queue is empty, and no end of execution has been specified.
                if (this._keepAlive) {
                    // Keep alive: snooze and wake up later.
                    Log.global.debug("Going to sleep.");
                    this.snooze.asSchedulable(this._getKey(this.snooze)).schedule(0, this._currentTag);
                } else {
                    // Don't keep alive: initiate shutdown.
                    Log.global.debug("Initiating shutdown.")
                    this._shutdown();
                }
            }
        }
    }

    /**
     * Disable the alarm and clear possible immediate next.
     */
    protected _cancelNext() {
        this._alarm.unset();
        if (this._immediateRef) {
            clearImmediate(this._immediateRef);
            this._immediateRef = undefined;
        }
        this._eventQ.empty()
    }

    /**
     * 
     * @param tag 
     */
    protected _setAlarmOrYield(tag: Tag) {
        Log.debug(this, () => {return "In setAlarmOrYield for tag: " + tag});
        
        if (this._endOfExecution && this._endOfExecution.isSmallerThan(tag)) {
            // Ignore this request if the tag is later than the end of execution.
            return;
        }
        
        let physicalTime = getCurrentPhysicalTime();
        let timeout = physicalTime.difference(tag.time);
        if (physicalTime.isEarlierThan(tag.time) && !this._fast) {
            // Set an alarm to be woken up when the event's tag matches physical
            // time.
            this._alarm.set(function (this: App) {
                this._next();
            }.bind(this), timeout)
        } else {
            // Either we're in "fast" mode, or we're lagging behind.
            this._requestImmediateInvocationOfNext();
        }
    }

    /**
     * Request an immediate invocation of `this._next()`.
     */
    protected _requestImmediateInvocationOfNext() {
        // Only schedule an immediate if none is already pending.
        if (!this._immediateRef) {
            this._immediateRef = setImmediate(function (this: App) {
                this._immediateRef = undefined;
                this._next()
            }.bind(this));
        }
    }  

    /**
     * Schedule a shutdown event at the current time if no such action has been taken yet. 
     * Clear the alarm, and set the end of execution to be the current tag. 
     */
    protected _shutdown(): void {
        if (this.__runtime.isRunning()) {
            this._endOfExecution = this._currentTag.getMicroStepLater() // FIXME: this could be a longer delay in distributed execution

            Log.debug(this, () => "Stop requested.");
            Log.debug(this, () => "Setting end of execution to: " + this._endOfExecution);

            this.schedulable(this.shutdown).schedule(0, null);

        } else {
            Log.global.debug("Ignoring App._shutdown() call after shutdown has already started.");
        }
    }

    /**
     * Wrap up execution by logging information and reporting errors if applicable.
     */
    private _done(): void {
        this._cancelNext();
        Log.info(this, () => Log.hr);
        Log.info(this, () => ">>> End of execution at (logical) time: " + this.util.getCurrentLogicalTime());
        Log.info(this, () => ">>> Elapsed physical time: " + this.util.getElapsedPhysicalTime());
        Log.info(this, () => Log.hr);

        if (this._errored) {
            this.failure()
            console.error(">>> Erroneous exit.")
            if (this._message) {
                console.error("Reason: " + this._message)
            }
        } else {
            this.success()
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
        let initStart = getCurrentPhysicalTime();
        Log.global.info(">>> Initializing");

        Log.global.debug("Initiating startup sequence.")
        
        // Obtain the precedence graph, ensure it has no cycles, 
        // and assign a priority to each reaction in the graph.
        var apg = this._getPrecedenceGraph();

        Log.debug(this, () => "Before collapse: " + apg.toString());
        var collapsed = new SortableDependencyGraph()

        // 1. Collapse dependencies and weed out the ports.
        let leafs = apg.leafNodes()
        let visited = new Set()

        function search(reaction: Reaction<unknown>, 
            nodes: Set<Port<Present> | Reaction<unknown>>) {
            for (let node of nodes) {    
                if (node instanceof Reaction) {
                    collapsed.addEdge(reaction, node)
                    if (!visited.has(node)) {
                        visited.add(node)
                        search(node, apg.getEdges(node))
                    }
                } else {
                    search(reaction, apg.getEdges(node))
                }
            }
        }

        for (let leaf of leafs) {
            if (leaf instanceof Reaction) {
                collapsed.addNode(leaf)
                search(leaf, apg.getEdges(leaf))
                visited.clear()
            }
        }        

        // 2. Update priorities.
        Log.debug(this, () => "After collapse: " + collapsed.toString());

        if (collapsed.updatePriorities(true)) {
            Log.global.debug("No cycles.");
        } else {
            throw new Error("Cycle in reaction graph.");
        }

        Log.info(this, () => ">>> Spent " + getCurrentPhysicalTime().subtract(initStart as TimeValue)
            + " checking the precedence graph.");
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
    protected _determineStartAndEndOfExecution(startTime: TimeValue) {
        // Let the start of the execution be the current physical time.
        this._startOfExecution = startTime;
        this._currentTag = new Tag(this._startOfExecution, 0);

        // Mark the app as active, now that the start time is known.
        this._active = true;

        // Schedule all timers created during the instantiation of this app.
        this._timersToSchedule.forEach(timer => this.__runtime.initialize(timer))

        if (this._executionTimeout != null) {
            this._endOfExecution = new Tag(this._startOfExecution.add(this._executionTimeout),0);
            Log.debug(this, () => "Execution timeout: " + this._executionTimeout);

            // If there is a known end of execution, schedule a shutdown reaction to that effect.
            this.__runtime.schedule(new TaggedEvent(this.shutdown, this._endOfExecution, null));
        }
    }

    /**
     * Load all reactions that were staged for immediate execution during this
     * app's instantiation onto the reaction queue.
     */
    protected _loadStartupReactions() {
        this._reactionsAtStartup.forEach(r => this._reactionQ.push(r))
    }

    /**
     * Start executing reactions. 
     */
    protected _startExecuting() {

        Log.info(this, () => Log.hr);
        Log.info(this, () => Log.hr);
        
        Log.info(this, () => ">>> Start of execution: " + this._currentTag);
        Log.info(this, () => Log.hr);

        // Handle the reactions that were loaded onto the reaction queue.
        this._react()

        // Continue execution by processing the next event.
        this._next()
    }
    
    /**
     * Start the app.
     */
    public _start(): void {

        // First analyze the dependency graph to determine whether it is valid.
        this._analyzeDependencies()

        // Then load any reactions that were staged during the instantiation of
        // any of the reactors.
        this._loadStartupReactions()        

        // Use the current physical time to set the app's start of execution.
        this._determineStartAndEndOfExecution(getCurrentPhysicalTime());
        
        // Start the main event loop.
        this._startExecuting()
    }
}
