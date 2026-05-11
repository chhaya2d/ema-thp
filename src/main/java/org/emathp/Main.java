package org.emathp;

import org.emathp.config.RuntimeEnv;
import org.emathp.demo.SnapshotFileDemo;

/** CLI entry point. Mock-connector SQL scenarios are regression-tested in {@code org.emathp.demo.FederatedDemosTest}. */
public final class Main {

    private Main() {}

    public static void main(String[] args) {
        RuntimeEnv.loadDotEnv();
        if (args.length > 0 && "web".equalsIgnoreCase(args[0])) {
            try {
                org.emathp.web.DemoWebServer.run();
            } catch (Exception e) {
                e.printStackTrace(System.err);
                System.exit(1);
            }
            return;
        }
        if (args.length > 0 && "snapshot-demo-mock".equalsIgnoreCase(args[0])) {
            SnapshotFileDemo.runMockDemo();
            return;
        }

        System.out.println("=== EmaTHP - Federated query engine ===\n");
        System.out.println("Mock-connector SQL scenarios (pushdown + executor rows) live in tests:");
        System.out.println("  org.emathp.demo.FederatedDemosTest\n");
        System.out.println("Run:");
        System.out.println("  gradlew test --tests org.emathp.demo.FederatedDemosTest");
        System.out.println("\nFilesystem snapshot demo (mock connectors, data/test):");
        System.out.println("  gradlew run --args snapshot-demo-mock");
        System.out.println("\nStart the demo HTTP server:");
        System.out.println("  gradlew run --args web");
    }
}
