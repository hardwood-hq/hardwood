/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

import dev.hardwood.internal.reader.engine.micro.runtime.Interpreter;

/// How the read's commands run: a fetch is I/O, a decode is CPU, and both call the file. The only
/// place that does.
final class ReadInterpreter implements Interpreter<Command, Fact> {

    @Override
    public Resource resource(Command command) {
        return switch (command) {
            case Command.Fetch ignored -> Resource.IO;
            case Command.FetchIndex ignored -> Resource.IO;
            case Command.ParseIndex ignored -> Resource.CPU;
            case Command.Decode ignored -> Resource.CPU;
        };
    }

    @Override
    public Fact execute(Command command) throws InterruptedException {
        return switch (command) {
            case Command.Fetch f -> {
                SimulatedFile.fetch(f.request());
                yield new Fact.Fetched(f.request());
            }
            case Command.FetchIndex f -> {
                SimulatedFile.fetchIndex(f.rowGroup());
                yield new Fact.IndexFetched(f.rowGroup());
            }
            case Command.ParseIndex p -> {
                SimulatedFile.parseIndex(p.rowGroup());
                yield new Fact.IndexParsed(p.rowGroup());
            }
            case Command.Decode d -> new Fact.Decoded(d.page(), SimulatedFile.decode(d.page()));
        };
    }

    @Override
    public Fact failed(Command command, Throwable cause) {
        return new Fact.Failed(command, cause);
    }
}
