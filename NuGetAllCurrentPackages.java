import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class NuGetPackageVersions {

    private static final String CATALOG_INDEX =
            "https://api.nuget.org/v3/catalog0/index.json";

    private static final String OUTPUT =
            "D:\\nuget_java\\output\\NuGet_Package_Name_Versions.csv";

    private static final int WORKERS = 32;

    // package -> versions
    private static final ConcurrentHashMap<String, Set<String>> DATA =
            new ConcurrentHashMap<>();

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .version(HttpClient.Version.HTTP_2)
            .build();

    public static void main(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        System.out.println("==============================================");
        System.out.println(" NUGet Package + Version Extractor");
        System.out.println("==============================================");
        System.out.println("Workers : " + WORKERS);

        System.out.println();
        System.out.println("[1/3] Reading catalog index...");

        List<PageInfo> pages = readCatalogPages();

        System.out.println("Catalog pages : " + pages.size());

        System.out.println();
        System.out.println("[2/3] Extracting Package Name + Version...");

        ExecutorService pool = Executors.newFixedThreadPool(WORKERS);

        List<Future<?>> futures = new ArrayList<>();

        int count = 0;

        for (PageInfo page : pages) {

            futures.add(pool.submit(() -> processPage(page)));

            count++;

            if (count % 500 == 0 || count == pages.size()) {
                System.out.printf(
                        "Pages submitted: %,d / %,d%n",
                        count,
                        pages.size()
                );
            }
        }

        for (Future<?> future : futures) {
            future.get();
        }

        pool.shutdown();

        long totalVersions = DATA.values()
                .stream()
                .mapToLong(Set::size)
                .sum();

        System.out.println();
        System.out.println("Unique Packages : " + DATA.size());
        System.out.println("Unique Versions : " + totalVersions);

        System.out.println();
        System.out.println("[3/3] Writing CSV...");

        writeCsv();

        long seconds =
                (System.currentTimeMillis() - start) / 1000;

        System.out.println();
        System.out.println("==============================================");
        System.out.println(" DONE");
        System.out.println("==============================================");
        System.out.println("Unique Packages : " + DATA.size());
        System.out.println("Unique Versions : " + totalVersions);
        System.out.println("CSV:");
        System.out.println(OUTPUT);
        System.out.println("Time : " + seconds + " seconds");
    }

    // ---------------------------------------------------------
    // Read catalog index
    // ---------------------------------------------------------

    private static List<PageInfo> readCatalogPages() throws Exception {

        String json = download(CATALOG_INDEX);

        JsonFactory factory = new JsonFactory();

        List<PageInfo> pages = new ArrayList<>();

        try (JsonParser parser =
                     factory.createParser(json)) {

            while (!parser.isClosed()) {

                JsonToken token = parser.nextToken();

                if (token == null)
                    break;

                if (token == JsonToken.START_OBJECT) {

                    String id = null;
                    String timestamp = null;

                    while (parser.nextToken() != JsonToken.END_OBJECT) {

                        String field = parser.getCurrentName();

                        parser.nextToken();

                        if ("@id".equals(field)) {
                            id = parser.getValueAsString();
                        }
                        else if ("commitTimeStamp".equals(field)) {
                            timestamp = parser.getValueAsString();
                        }
                        else {
                            parser.skipChildren();
                        }
                    }

                    if (id != null && timestamp != null) {
                        pages.add(new PageInfo(id, timestamp));
                    }
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

    // ---------------------------------------------------------
    // Process catalog page
    // ---------------------------------------------------------

    private static void processPage(PageInfo page) {

        try {

            String json = downloadWithRetry(page.url);

            JsonFactory factory = new JsonFactory();

            try (JsonParser parser =
                         factory.createParser(json)) {

                String packageId = null;
                String version = null;
                String type = null;

                while (!parser.isClosed()) {

                    JsonToken token = parser.nextToken();

                    if (token == null)
                        break;

                    if (token == JsonToken.FIELD_NAME) {

                        String field = parser.getCurrentName();

                        parser.nextToken();

                        if ("nuget:id".equals(field)) {

                            packageId =
                                    parser.getValueAsString();
                        }
                        else if ("nuget:version".equals(field)) {

                            version =
                                    parser.getValueAsString();
                        }
                        else if ("@type".equals(field)) {

                            if (parser.currentToken() ==
                                    JsonToken.START_ARRAY) {

                                StringBuilder types =
                                        new StringBuilder();

                                while (parser.nextToken() !=
                                        JsonToken.END_ARRAY) {

                                    if (parser.currentToken() ==
                                            JsonToken.VALUE_STRING) {

                                        types.append(
                                                parser.getValueAsString()
                                        ).append(" ");
                                    }
                                }

                                type = types.toString();
                            }
                            else {

                                type =
                                        parser.getValueAsString();
                            }
                        }

                        if (packageId != null &&
                                version != null) {

                            // Ignore delete events
                            if (type == null ||
                                    !type.contains("PackageDelete")) {

                                if (isValidPackage(packageId) &&
                                        isValidVersion(version)) {

                                    DATA.computeIfAbsent(
                                            packageId,
                                            k -> ConcurrentHashMap.newKeySet()
                                    ).add(version);
                                }
                            }

                            packageId = null;
                            version = null;
                            type = null;
                        }
                    }
                }
            }

        } catch (Exception e) {

            System.err.println(
                    "Failed page: " + page.url
            );
        }
    }

    // ---------------------------------------------------------
    // CSV
    // ---------------------------------------------------------

    private static void writeCsv() throws Exception {

        Path output = Paths.get(OUTPUT);

        Files.createDirectories(output.getParent());

        Path temp = Paths.get(
                OUTPUT + ".tmp"
        );

        try (
                BufferedWriter writer =
                        Files.newBufferedWriter(
                                temp,
                                StandardCharsets.UTF_8
                        )
        ) {

            writer.write("Package Name,Version");
            writer.newLine();

            List<String> packages =
                    new ArrayList<>(DATA.keySet());

            Collections.sort(
                    packages,
                    String.CASE_INSENSITIVE_ORDER
            );

            for (String packageName : packages) {

                List<String> versions =
                        new ArrayList<>(
                                DATA.get(packageName)
                        );

                versions.sort(
                        NuGetPackageVersions::compareVersions
                );

                for (String version : versions) {

                    writer.write(
                            csv(packageName)
                    );

                    writer.write(",");

                    writer.write(
                            csv(version)
                    );

                    writer.newLine();
                }
            }
        }

        Files.move(
                temp,
                output,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    // ---------------------------------------------------------
    // Download
    // ---------------------------------------------------------

    private static String download(String url)
            throws Exception {

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofMinutes(2))
                        .header("User-Agent",
                                "NuGetPackageExtractor/1.0")
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
                    "HTTP " + response.statusCode()
            );
        }

        return response.body();
    }

    private static String downloadWithRetry(String url) {

        for (int attempt = 1; attempt <= 5; attempt++) {

            try {
                return download(url);
            }
            catch (Exception e) {

                try {
                    Thread.sleep(
                            1000L * attempt
                    );
                }
                catch (InterruptedException ignored) {
                }
            }
        }

        throw new RuntimeException(
                "Failed after retries: " + url
        );
    }

    // ---------------------------------------------------------
    // Validation
    // ---------------------------------------------------------

    private static boolean isValidPackage(String id) {

        if (id == null || id.isEmpty())
            return false;

        if (id.length() > 128)
            return false;

        if (id.contains("$"))
            return false;

        if (id.matches(".*\\s+.*"))
            return false;

        return true;
    }

    private static boolean isValidVersion(String version) {

        if (version == null || version.isEmpty())
            return false;

        if (version.length() > 200)
            return false;

        if (version.matches(".*\\s+.*"))
            return false;

        return true;
    }

    // ---------------------------------------------------------
    // CSV escaping
    // ---------------------------------------------------------

    private static String csv(String value) {

        if (value.contains(",") ||
                value.contains("\"") ||
                value.contains("\n") ||
                value.contains("\r")) {

            return "\"" +
                    value.replace("\"", "\"\"") +
                    "\"";
        }

        return value;
    }

    // ---------------------------------------------------------
    // Version sorting
    // ---------------------------------------------------------

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

            if (i >= aa.length)
                return -1;

            if (i >= bb.length)
                return 1;

            String x = aa[i];
            String y = bb[i];

            try {

                long nx =
                        Long.parseLong(x);

                long ny =
                        Long.parseLong(y);

                int c =
                        Long.compare(nx, ny);

                if (c != 0)
                    return c;

            }
            catch (NumberFormatException e) {

                int c =
                        x.compareToIgnoreCase(y);

                if (c != 0)
                    return c;
            }
        }

        return a.compareToIgnoreCase(b);
    }

    // ---------------------------------------------------------
    // Page
    // ---------------------------------------------------------

    private static class PageInfo {

        String url;
        String timestamp;

        PageInfo(
                String url,
                String timestamp) {

            this.url = url;
            this.timestamp = timestamp;
        }
    }
}