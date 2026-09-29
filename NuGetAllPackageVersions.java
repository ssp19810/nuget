import com.fasterxml.jackson.core.*;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class NuGetAllPackageVersions {

    private static final String CATALOG_INDEX =
            "https://api.nuget.org/v3/catalog0/index.json";

    private static final String OUTPUT =
            "D:\\nuget_java\\output\\NuGet_All_Package_Versions.csv";

    private static final int WORKERS = 24;

    private static final HttpClient CLIENT =
            HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(30))
                    .version(HttpClient.Version.HTTP_2)
                    .build();

    /*
     * Key:
     *      packageId + separator + version
     *
     * Value:
     *      latest catalog event
     *
     * This prevents duplicate package/version pairs and
     * allows PackageDelete to override an earlier PackageDetails.
     */
    private static final ConcurrentHashMap<String, Event> EVENTS =
            new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        System.out.println();
        System.out.println("================================================");
        System.out.println(" NuGet ALL PACKAGE + VERSION EXTRACTOR");
        System.out.println("================================================");
        System.out.println("Workers : " + WORKERS);
        System.out.println();

        // ----------------------------------------------------
        // 1. Catalog index
        // ----------------------------------------------------

        System.out.println("[1/4] Downloading catalog index...");

        List<PageInfo> pages = readCatalogPages();

        System.out.println("Catalog pages : " + pages.size());

        if (pages.isEmpty()) {
            throw new RuntimeException(
                    "No catalog pages found."
            );
        }

        // ----------------------------------------------------
        // 2. Process catalog pages
        // ----------------------------------------------------

        System.out.println();
        System.out.println("[2/4] Processing catalog pages...");

        ExecutorService executor =
                Executors.newFixedThreadPool(WORKERS);

        AtomicCounter counter =
                new AtomicCounter();

        List<Future<?>> futures =
                new ArrayList<>(pages.size());

        for (PageInfo page : pages) {

            futures.add(
                    executor.submit(() -> {

                        processPage(page);

                        int n = counter.increment();

                        if (n % 100 == 0 ||
                                n == pages.size()) {

                            System.out.printf(
                                    "Pages: %,d / %,d | Package-Version events: %,d%n",
                                    n,
                                    pages.size(),
                                    EVENTS.size()
                            );
                        }
                    })
            );
        }

        for (Future<?> future : futures) {
            future.get();
        }

        executor.shutdown();

        // ----------------------------------------------------
        // 3. Count current non-deleted pairs
        // ----------------------------------------------------

        long packageVersionCount = EVENTS.values()
                .stream()
                .filter(e -> !e.deleted)
                .count();

        Set<String> packages = new HashSet<>();

        for (Event event : EVENTS.values()) {

            if (!event.deleted) {
                packages.add(event.packageId);
            }
        }

        System.out.println();
        System.out.println("Unique Packages       : " + packages.size());
        System.out.println("Package-Version Rows  : " + packageVersionCount);

        // ----------------------------------------------------
        // 4. Write CSV
        // ----------------------------------------------------

        System.out.println();
        System.out.println("[3/4] Writing CSV...");

        writeCsv();

        long seconds =
                (System.currentTimeMillis() - start) / 1000;

        System.out.println();
        System.out.println("[4/4] Finished.");
        System.out.println();
        System.out.println("================================================");
        System.out.println(" DONE");
        System.out.println("================================================");
        System.out.println("Unique Packages      : " + packages.size());
        System.out.println("Package-Version Rows : " + packageVersionCount);
        System.out.println("CSV:");
        System.out.println(OUTPUT);
        System.out.println("Time : " + seconds + " seconds");
        System.out.println("================================================");
    }

    // ========================================================
    // Read catalog index
    // ========================================================

    private static List<PageInfo> readCatalogPages()
            throws Exception {

        String json = download(CATALOG_INDEX);

        JsonFactory factory = new JsonFactory();

        List<PageInfo> pages = new ArrayList<>();

        try (JsonParser parser =
                     factory.createParser(json)) {

            while (parser.nextToken() != null) {

                if (parser.currentToken() !=
                        JsonToken.START_OBJECT) {
                    continue;
                }

                String id = null;
                String timestamp = null;

                while (parser.nextToken() !=
                        JsonToken.END_OBJECT) {

                    String field =
                            parser.getCurrentName();

                    parser.nextToken();

                    if ("@id".equals(field)) {

                        id = parser.getValueAsString();

                    } else if ("commitTimeStamp".equals(field)) {

                        timestamp =
                                parser.getValueAsString();

                    } else {

                        parser.skipChildren();
                    }
                }

                if (id != null &&
                        timestamp != null &&
                        id.contains("/catalog")) {

                    pages.add(
                            new PageInfo(
                                    id,
                                    timestamp
                            )
                    );
                }
            }
        }

        pages.sort(
                Comparator.comparing(
                        p -> p.timestamp
                )
        );

        return pages;
    }

    // ========================================================
    // Process one catalog page
    // ========================================================

    private static void processPage(
            PageInfo page) {

        try {

            String json =
                    downloadRetry(page.url);

            JsonFactory factory =
                    new JsonFactory();

            try (JsonParser parser =
                         factory.createParser(json)) {

                String packageId = null;
                String version = null;
                String type = null;
                String timestamp = null;

                while (parser.nextToken() != null) {

                    if (parser.currentToken() !=
                            JsonToken.FIELD_NAME) {

                        continue;
                    }

                    String field =
                            parser.getCurrentName();

                    parser.nextToken();

                    if ("nuget:id".equals(field)) {

                        packageId =
                                parser.getValueAsString();

                    } else if ("nuget:version".equals(field)) {

                        version =
                                parser.getValueAsString();

                    } else if ("commitTimeStamp".equals(field)) {

                        timestamp =
                                parser.getValueAsString();

                    } else if ("@type".equals(field)) {

                        type =
                                readType(parser);
                    }

                    /*
                     * A catalog item is complete once we have:
                     *
                     * package ID
                     * version
                     * type
                     *
                     * timestamp can be inherited from page
                     * if the leaf doesn't contain it.
                     */
                    if (packageId != null &&
                            version != null &&
                            type != null) {

                        if (timestamp == null) {
                            timestamp =
                                    page.timestamp;
                        }

                        processEvent(
                                packageId,
                                version,
                                type,
                                timestamp
                        );

                        packageId = null;
                        version = null;
                        type = null;
                        timestamp = null;
                    }
                }
            }

        } catch (Exception e) {

            System.err.println(
                    "ERROR page: " +
                    page.url +
                    " -> " +
                    e.getMessage()
            );
        }
    }

    // ========================================================
    // Read @type
    // ========================================================

    private static String readType(
            JsonParser parser)
            throws IOException {

        if (parser.currentToken() ==
                JsonToken.START_ARRAY) {

            StringBuilder result =
                    new StringBuilder();

            while (parser.nextToken() !=
                    JsonToken.END_ARRAY) {

                if (parser.currentToken() ==
                        JsonToken.VALUE_STRING) {

                    if (result.length() > 0) {
                        result.append("|");
                    }

                    result.append(
                            parser.getValueAsString()
                    );
                }
            }

            return result.toString();

        } else {

            return parser.getValueAsString();
        }
    }

    // ========================================================
    // Process event
    // ========================================================

    private static void processEvent(
            String packageId,
            String version,
            String type,
            String timestamp) {

        if (!validPackageId(packageId) ||
                !validVersion(version)) {

            return;
        }

        boolean deleted =
                type.contains("PackageDelete");

        String key =
                packageId.toLowerCase(Locale.ROOT)
                        + "\u0001"
                        + version.toLowerCase(Locale.ROOT);

        Event newEvent =
                new Event(
                        packageId,
                        version,
                        timestamp,
                        deleted
                );

        EVENTS.merge(
                key,
                newEvent,
                (oldEvent, newEventValue) -> {

                    if (oldEvent.timestamp == null) {
                        return newEventValue;
                    }

                    if (newEventValue.timestamp == null) {
                        return oldEvent;
                    }

                    if (newEventValue.timestamp.compareTo(
                            oldEvent.timestamp) > 0) {

                        return newEventValue;
                    }

                    return oldEvent;
                }
        );
    }

    // ========================================================
    // CSV
    // ========================================================

    private static void writeCsv()
            throws Exception {

        Path output =
                Paths.get(OUTPUT);

        Files.createDirectories(
                output.getParent()
        );

        Path temp =
                Paths.get(
                        OUTPUT + ".tmp"
                );

        List<Event> results =
                new ArrayList<>();

        for (Event event : EVENTS.values()) {

            if (!event.deleted) {
                results.add(event);
            }
        }

        /*
         * Sort by package name, then version.
         */
        results.sort(
                (a, b) -> {

                    int c =
                            a.packageId.compareToIgnoreCase(
                                    b.packageId
                            );

                    if (c != 0) {
                        return c;
                    }

                    return compareVersions(
                            a.version,
                            b.version
                    );
                }
        );

        try (BufferedWriter writer =
                     Files.newBufferedWriter(
                             temp,
                             StandardCharsets.UTF_8
                     )) {

            writer.write(
                    "Package Name,Version"
            );

            writer.newLine();

            for (Event event : results) {

                writer.write(
                        csv(event.packageId)
                );

                writer.write(",");

                writer.write(
                        csv(event.version)
                );

                writer.newLine();
            }
        }

        Files.move(
                temp,
                output,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    // ========================================================
    // HTTP
    // ========================================================

    private static String download(
            String url)
            throws Exception {

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(
                                Duration.ofMinutes(2)
                        )
                        .header(
                                "User-Agent",
                                "NuGet-Java-Extractor/1.0"
                        )
                        .GET()
                        .build();

        HttpResponse<String> response =
                CLIENT.send(
                        request,
                        HttpResponse.BodyHandlers.ofString(
                                StandardCharsets.UTF_8
                        )
                );

        if (response.statusCode() != 200) {

            throw new IOException(
                    "HTTP " +
                    response.statusCode()
            );
        }

        return response.body();
    }

    private static String downloadRetry(
            String url) {

        for (int attempt = 1;
             attempt <= 5;
             attempt++) {

            try {

                return download(url);

            } catch (Exception e) {

                try {

                    Thread.sleep(
                            1000L * attempt
                    );

                } catch (InterruptedException ignored) {
                }
            }
        }

        throw new RuntimeException(
                "Failed after 5 attempts: " +
                url
        );
    }

    // ========================================================
    // Validation
    // ========================================================

    private static boolean validPackageId(
            String id) {

        return id != null &&
                !id.isEmpty() &&
                id.length() <= 128 &&
                !id.contains("$") &&
                !id.matches(".*\\s+.*");
    }

    private static boolean validVersion(
            String version) {

        return version != null &&
                !version.isEmpty() &&
                version.length() <= 200 &&
                !version.matches(".*\\s+.*");
    }

    // ========================================================
    // CSV escaping
    // ========================================================

    private static String csv(
            String value) {

        if (value.contains(",") ||
                value.contains("\"") ||
                value.contains("\n") ||
                value.contains("\r")) {

            return "\"" +
                    value.replace(
                            "\"",
                            "\"\""
                    ) +
                    "\"";
        }

        return value;
    }

    // ========================================================
    // Version sorting
    // ========================================================

    private static int compareVersions(
            String a,
            String b) {

        String[] aa =
                a.split("[.-]");

        String[] bb =
                b.split("[.-]");

        int max =
                Math.max(
                        aa.length,
                        bb.length
                );

        for (int i = 0; i < max; i++) {

            if (i >= aa.length) {
                return -1;
            }

            if (i >= bb.length) {
                return 1;
            }

            String x = aa[i];
            String y = bb[i];

            try {

                long nx =
                        Long.parseLong(x);

                long ny =
                        Long.parseLong(y);

                int c =
                        Long.compare(nx, ny);

                if (c != 0) {
                    return c;
                }

            } catch (NumberFormatException e) {

                int c =
                        x.compareToIgnoreCase(y);

                if (c != 0) {
                    return c;
                }
            }
        }

        return a.compareToIgnoreCase(b);
    }

    // ========================================================
    // Classes
    // ========================================================

    private static class PageInfo {

        final String url;
        final String timestamp;

        PageInfo(
                String url,
                String timestamp) {

            this.url = url;
            this.timestamp = timestamp;
        }
    }

    private static class Event {

        final String packageId;
        final String version;
        final String timestamp;
        final boolean deleted;

        Event(
                String packageId,
                String version,
                String timestamp,
                boolean deleted) {

            this.packageId = packageId;
            this.version = version;
            this.timestamp = timestamp;
            this.deleted = deleted;
        }
    }

    private static class AtomicCounter {

        private int value = 0;

        synchronized int increment() {
            return ++value;
        }
    }
}