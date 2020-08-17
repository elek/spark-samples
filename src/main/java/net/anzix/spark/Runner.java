package net.anzix.spark;

import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "runner",
        subcommands = {Generate.class, Copy.class, Count.class},
        mixinStandardHelpOptions = true,
        description = "Executes spark example jobs.")
public class Runner implements Callable<Integer> {

    private static final CommandLine COMMAND_LINE = new CommandLine(new Runner());

    public static void main(String... args) {
        int exitCode = COMMAND_LINE.execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception { // your business logic goes here...
        throw new IllegalArgumentException("Choose a subcommand");
    }
}