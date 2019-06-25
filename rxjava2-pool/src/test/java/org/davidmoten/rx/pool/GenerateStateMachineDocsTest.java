package org.davidmoten.rx.pool;

import org.junit.Test;

import com.github.davidmoten.fsm.model.State;
import com.github.davidmoten.fsm.model.StateMachineDefinition;
import com.github.davidmoten.fsm.runtime.Event;

public class GenerateStateMachineDocsTest {

    @Test
    public void test() {
        StateMachineDefinition<Member> m = StateMachineDefinition.create(Member.class);
        State<Member, CreateMember> notInitialized = //
                m.createState("Not Initialized").event(CreateMember.class);
        State<Member, CreateValue> initializing = //
                m.createState("Initializing").event(CreateValue.class);
        State<Member, ValueCreated> initializedAvailable = //
                m.createState("Initialized Available ").event(ValueCreated.class);
        State<Member, CheckHealth> checkingHealth = //
                m.createState("Checking Health").event(CheckHealth.class);
        State<Member, Release> releasing = //
                m.createState("Releasing").event(Release.class);
        State<Member, CheckHealthThreshold> checkingHealthThreshold = //
                m.createState("Checking Health Threshold").event(CheckHealthThreshold.class);
        State<Member, Checkout> checkedOut = //
                m.createState("Checked Out").event(Checkout.class);
        notInitialized //
                .from(releasing.from(initializedAvailable)) //
                .to(initializing) //
                .to(initializedAvailable) //
                .from(checkedOut) //
                .to(checkingHealthThreshold) //
                .to(checkingHealth) //
                .to(checkedOut);
    }

    public static final class Member {
    }

    public static final class CreateMember implements Event<Member> {
    }

    public static final class CreateValue implements Event<Member> {
    }

    public static final class ValueCreated implements Event<Member> {
    }

    public static final class CheckHealthThreshold implements Event<Member> {
    }

    public static final class CheckHealth implements Event<Member> {
    }

    public static final class Clear implements Event<Member> {
    }

    public static final class Release implements Event<Member> {
    }

    public static final class Checkin implements Event<Member> {
    }

    public static final class Checkout implements Event<Member> {
    }
}
