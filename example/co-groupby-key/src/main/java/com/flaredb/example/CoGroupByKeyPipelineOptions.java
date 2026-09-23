package com.flaredb.example;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;

import com.flaredb.runner.FlarePipelineOptions;
import com.flaredb.runner.FlareRunner;

public interface CoGroupByKeyPipelineOptions extends FlarePipelineOptions {

    /**
     * Applies the standard FlareDB example defaults: run on {@link FlareRunner}, target the local
     * FlareDB job service, and auto-detect the shadow (uber) JAR produced by this module's {@code
     * shadowJar} task. An explicitly configured {@code --uberJar} takes precedence.
     *
     * @return the same options, for fluent use
     */
    static CoGroupByKeyPipelineOptions applyFlareDefaults(CoGroupByKeyPipelineOptions options) {
        options.setRunner(FlareRunner.class);
        options.setJobEndpoint("127.0.0.1:8099");
        if (options.getUberJar() == null || options.getUberJar().isEmpty()) {
            File shadowJar = findShadowJar();
            if (shadowJar != null) {
                options.setUberJar(shadowJar.getAbsolutePath());
            }
        }
        return options;
    }

    /** Locates the shadow (uber) JAR produced by this module's {@code shadowJar} task. */
    private static File findShadowJar() {
        String[] candidateDirs = {"build/libs", "example/co-groupby-key/build/libs"};
        for (String dir : candidateDirs) {
            File[] matches =
                    new File(dir)
                            .listFiles(
                                    (d, name) ->
                                            name.startsWith("co-groupby-key-") && name.endsWith("-all.jar"));
            if (matches != null && matches.length > 0) {
                Arrays.sort(matches, Comparator.comparing(File::getName));
                return matches[matches.length - 1];
            }
        }
        return null;
    }
}
