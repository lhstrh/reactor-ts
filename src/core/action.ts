import { TaggedEvent } from "./event";
import { Absent, Present, Reactor, Read, Sched, SchedulableAction } from "./reactor";
import { getCurrentPhysicalTime, Origin, Tag, TimeUnit, TimeValue } from "./time";
import { ScheduledTrigger, TriggerManager } from "./trigger";
import { Log } from "./util";

const defaultMIT = TimeValue.withUnits(1, TimeUnit.nsec); // FIXME


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

    public asSchedulable(key: Symbol | undefined): Sched<T> {
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
        schedule(extraDelay: 0 | TimeValue, value: T, intendedTag?: Tag): void {
            if (!(extraDelay instanceof TimeValue)) {
                extraDelay = TimeValue.secs(0);
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

            if (this.action instanceof FederatePortAction) {
                if (intendedTag === undefined) {
                    throw new Error("FederatedPortAction must have an intended tag from RTI.");
                }
                if (intendedTag <= this.action.runtime.util.getCurrentTag()) {
                    throw new Error("Intended tag must be greater than current tag. Intended tag" +
                    intendedTag + " Current tag: " + this.action.runtime.util.getCurrentTag());
                }
                Log.debug(this, () => "Using intended tag from RTI, similar to schedule_at_tag(tag) with an intended tag: " +
                intendedTag);
                tag = intendedTag;
            } else if (this.action.origin == Origin.logical && !(this.action instanceof Startup)) {
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
    constructor(__container__: Reactor, origin: Origin, minDelay: TimeValue = TimeValue.secs(0), minInterArrival: TimeValue = defaultMIT) {
        super(__container__);
        this.origin = origin;
        this.minDelay = minDelay;
    }

    public toString() {
        return this._getFullyQualifiedName();
    }
}

// FIXME(marten): move these to trigger.ts and let them extend trigger

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

export class FederatePortAction extends Action<Buffer> {
    constructor(__parent__: Reactor) {
        super(__parent__, Origin.logical)
    }
}