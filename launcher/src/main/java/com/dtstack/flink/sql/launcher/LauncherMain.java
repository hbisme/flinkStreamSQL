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

 

package com.dtstack.flink.sql.launcher;

import avro.shaded.com.google.common.collect.Lists;
import com.dtstack.flink.sql.Main;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import java.io.File;
import java.util.List;
import com.dtstack.flink.sql.ClusterMode;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

/**
 * Date: 2017/2/20
 * Company: www.dtstack.com
 * @author xuchao
 */

public class LauncherMain {

    private static final String CORE_JAR = "core.jar";

    private static String SP = File.separator;


    private static String getLocalCoreJarPath(String localSqlRootJar){
        return localSqlRootJar + SP + CORE_JAR;
    }

    public static void main(String[] args) throws Exception {
        LauncherOptionParser optionParser = new LauncherOptionParser(args);
        LauncherOptions launcherOptions = optionParser.getLauncherOptions();
        String mode = launcherOptions.getMode();
        // 将参数转成list类型的数据(sql参数文件,已经转成sql文件中的内容)
        List<String> argList = optionParser.getProgramExeArgList();
        if(mode.equals(ClusterMode.local.name())) {
            // 将参数list转成 参数数组
            String[] localArgs = argList.toArray(new String[argList.size()]);
            Main.main(localArgs);
        } else {
            // 返回 ClusterClient(standalone 或者 yarn (哪种模式?))
            ClusterClient clusterClient = ClusterClientFactory.createClusterClient(launcherOptions);
            String pluginRoot = launcherOptions.getLocalSqlPluginPath();
            // jarFile 是core.jar
            File jarFile = new File(getLocalCoreJarPath(pluginRoot));
            String[] remoteArgs = argList.toArray(new String[argList.size()]);
            PackagedProgram program = new PackagedProgram(jarFile, Lists.newArrayList(), remoteArgs);
            if(StringUtils.isNotBlank(launcherOptions.getSavePointPath())){
                program.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(launcherOptions.getSavePointPath(), BooleanUtils.toBoolean(launcherOptions.getAllowNonRestoredState())));
            }
            // 真正开始运行程序
            clusterClient.run(program, 1);
            clusterClient.shutdown();
        }
    }
}
