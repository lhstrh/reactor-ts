import {App} from '../src/core/reactor';
import {Origin, TimeInterval} from '../src/core/time';
import {Reactor, Reaction, Timer, Action, Schedulable} from '../src/core/reactor';

//Upon initialization, this reactor should produce an
//output event
export class ActionTrigger extends Reactor {

    t1: Timer = new Timer(this, 0,0);
    
    // This action is scheduled with a value.
    a1: Action<string> = new Action<string>(this, Origin.logical);

    // This action is never scheduled. It should never be present.
    a2: Action<string> = new Action<string>(this, Origin.logical);

    constructor(parent:Reactor|null) {
        super(parent);
        //Reaction priorities matter here. The overridden reaction must go first.
        this.addReaction(new class<T> extends Reaction<T> {
            /**
             * Schedule the incorrect payload for action a1.
             * @override
             */
            //@ts-ignore
            react(a1: Schedulable<string>){
                a1.schedule(0, "goodbye");
                console.log("Scheduling the overridden action in ScheduleOverriddenAction to trigger RespondToAction");
            }
        }(this, this.check(this.t1), this.check(this.getSchedulable(this.a1), this.a2)));
        
        this.addReaction(new class<T> extends Reaction<T> {
            /**
             * Schedule the correct payload for action a1.
             * @override
             */
            //@ts-ignore
            react(a1: Schedulable<string>){
                a1.schedule(0, "hello");
                console.log("Scheduling the final action in ScheduleAction to trigger RespondToAction");
            }
        }(this, this.check(this.t1), this.check(this.getSchedulable(this.a1), this.a2)));

        this.addReaction(new class<T> extends Reaction<T> {
            /**
             * If the action payload is correct, test is successful. Otherwise it fails.
             * Since a2 was not scheduled it should return null on a call to get() and
             * should return false for isPresent().
             * @override
             */
            //@ts-ignore
            react(a1: Action<string>, a2: Action<string>){
                const msg = a1.get();
                const nothing = a2.get();
                if(msg == "hello" && nothing === null && ! a2.isPresent()) {
                    this.parent.util.success();
                    console.log("success")
                } else {
                    this.parent.util.failure();
                }
                console.log("Response to action is reacting. String payload is: " + msg);
            }
        }(this, this.check(this.a1), this.check(this.a1, this.a2)));
    }
}


class ActionTriggerTest extends App {
    aTrigger: ActionTrigger;

    constructor(name: string, timeout: TimeInterval, success?: ()=> void, fail?: ()=>void){
        super(timeout, success, fail);
        this.setAlias(name);
        this.aTrigger = new ActionTrigger(this);
    }
}

describe('ActionTrigger', function () {

    // Ensure the test will run for no more than 5 seconds.
    jest.setTimeout(5000);

    it('start runtime', done => {

        function failure(){
            throw new Error("Reactor has failed.");
        };

        //Tell the reactor runtime to successfully terminate after 3 seconds.
        var aTriggerTest = new ActionTriggerTest("ActionTriggerTest", new TimeInterval(3), done, failure);
        //Don't give the runtime the done callback because we don't care if it terminates
        aTriggerTest._start();

    })
});