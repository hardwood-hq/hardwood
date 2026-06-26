/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli;

import org.aesh.command.CommandResult;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;

@QuarkusMain
public class Main implements QuarkusApplication {

    public static void main(String... args) {
        Quarkus.run(Main.class, args);
    }

    @Override
    public int run(String... args) {
        try {
            CommandResult result = AeshCli.execute(args);
            return result.isSuccess() ? 0 : (result.getResultValue() != 0 ? result.getResultValue() : 1);
        } catch (Exception e) {
            System.err.println(e.getMessage() != null ? e.getMessage() : e.toString());
            return 1;
        }
    }
}
