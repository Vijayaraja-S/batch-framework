package com.p3.batchframework.utils;

public class Constants {
  public static final String DOT_ESCAPE_STRING = "{dot}";
  public static final String DOT_STRING = "\\.";
  public static final String JOB_NAME_KEY = "jobName";
  public static final String TYPE_SUFFIX = "_TYPE";
  public static final String ILLEGAL_STATE_MSG =
      "Illegal state (only happens on a race condition): " + "%s with name=%s and parameters=%s";
  public static final String JOB_EXECUTION_ALREADY_RUNNING = "job execution already running";
  public static final String JOB_NOT_RESTARTABLE = "job not restartable";
  public static final String JOB_ALREADY_COMPLETE = "job already complete";
}
