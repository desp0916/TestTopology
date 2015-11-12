package com.pic.ala.learn.TestTopology;

public @interface Debug {

	public enum Level{NONE, UNIT, INTEGRATION, FUNCTION};
	Level level() default Level.NONE;
	String name();
	String tester();
	String value() default "none";

}
