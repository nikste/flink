/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java;

//import org.apache.flink.api.java.JarHelper;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
//import org.apache.flink.api.scala.FlinkILoop;

import java.io.File;


/**
 * Created by Nikolaas Steenbergen on 23-4-15.
 */

public class ScalaShellRemoteEnvironment extends RemoteEnvironment {

    private final String host = null;

    private final int port = 0;

    private final String[] jarFiles = null;


    //private FlinkILoop flinkILoop;

    /**
     * Creates a new RemoteEnvironment that points to the master (JobManager) described by the
     * given host name and port.
     *
     * @param host The host name or address of the master (JobManager), where the program should be executed.
     * @param port The port of the master (JobManager), where the program should be executed.
     */
    public ScalaShellRemoteEnvironment(String host, int port) {
        super(host, port);
    }

    /**
     * Creates a new RemoteEnvironment that points to the master (JobManager) described by the
     * given host name and port.
     *
     * @param host     The host name or address of the master (JobManager), where the program should be executed.
     * @param port     The port of the master (JobManager), where the program should be executed.
     * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
     *                 user-defined functions, user-defined input formats, or any libraries, those must be
     */
    public ScalaShellRemoteEnvironment(String host, int port, String... jarFiles) {
        super(host, port, jarFiles);
    }

    /**
     * Creates new ScalaShellRemoteEnvironment that has a reference to the FlinkILoop
     * @param host     The host name or address of the master (JobManager), where the program should be executed.
     * @param port     The port of the master (JobManager), where the program should be executed.
     * @param flinkILoop The flink Iloop instance from which the ScalaShellRemoteEnvironment is called.
     */
    /*
    public ScalaShellRemoteEnvironment(String host, int port)//, FlinkILoop flinkILoop)
    {
        super(host,port,null);
        //this.flinkILoop = flinkILoop;
    }*/

    /**
     * custom execution function for shell commands, which will take precompiled jars and process them.
     * @param jobName
     * @param jarFiles
     * @return
     * @throws Exception
     */
    /*
    public JobExecutionResult execute(String jobName, String... jarFiles) throws Exception {
        Plan p = createProgramPlan(jobName);

        // jarr up
        String[] jf = {"test"};
        //super.execute();//(jobName,jf);
        PlanExecutor executor = PlanExecutor.createRemoteExecutor(host, port, jarFiles);
        executor.setPrintStatusDuringExecution(p.getExecutionConfig().isSysoutLoggingEnabled());
        return executor.executePlan(p);
    }*/


    /**
     * compiles jars on the fly
     * @param jobName
     * @return
     * @throws Exception
     */

    @Override
    public JobExecutionResult execute(String jobName)  throws Exception
    {

        Plan p = createProgramPlan(jobName);
        JarHelper jh = new JarHelper();



        // jarr up.
        File inFile = new File("/tmp/scala_shell/");
        File outFile = new File("/tmp/scala_shell_test.jar");
        jh.jarDir(inFile,outFile);

        System.out.println("jarred up!");

        System.out.println("outFile.toString = " + outFile.toString());

        String[] jarFiles = {outFile.toString()};

        PlanExecutor executor = PlanExecutor.createRemoteExecutor(host, port, jarFiles);
        executor.setPrintStatusDuringExecution(p.getExecutionConfig().isSysoutLoggingEnabled());
        return executor.executePlan(p);
    }
}