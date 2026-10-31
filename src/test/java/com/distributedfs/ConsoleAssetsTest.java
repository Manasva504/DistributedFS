package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Plain-Java checks of the web console's own files (milestone M4b-2), read from the class path
 * as the server reads them. They keep the rules the page's Content-Security-Policy and its
 * safety depend on, so that breaking one fails a test rather than only a browser:
 * <ul>
 *   <li>the HTML loads exactly one script and one style sheet, both from this server, and has no
 *       inline script, no style element or attribute and no event-handler attribute;</li>
 *   <li>the script never turns text into HTML (no innerHTML and the like), never evaluates code,
 *       never sets styles, and only names attributes from its own short list;</li>
 *   <li>nothing is loaded from another site: no URL anywhere except the SVG namespace name, no
 *       web fonts, no @import;</li>
 *   <li>every element the script looks up exists in the page.</li>
 * </ul>
 * Run: {@code java -cp "out;out-test;src\main\resources" com.distributedfs.ConsoleAssetsTest}.
 */
public class ConsoleAssetsTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("consoleassets-test-");

    private static final String SVG_NAMESPACE = "http://www.w3.org/2000/svg";

    public static void main(String[] args) {
        RUNNER.run("the three files are on the class path, valid UTF-8, and within the size limit", ConsoleAssetsTest::present);
        RUNNER.run("the page loads exactly one script and one style sheet, both from /console/", ConsoleAssetsTest::htmlLoads);
        RUNNER.run("the page has no inline script, no style element or attribute, and no event-handler attribute",
                ConsoleAssetsTest::htmlNoInline);
        RUNNER.run("the script never turns text into HTML and never evaluates code", ConsoleAssetsTest::jsNoHtmlSinks);
        RUNNER.run("the script never sets a style, and names only attributes from its own list", ConsoleAssetsTest::jsNoStyles);
        RUNNER.run("the script's attribute list has no style, event handler, src or srcdoc", ConsoleAssetsTest::jsAttributeList);
        RUNNER.run("the script talks only to this server, with same-origin requests", ConsoleAssetsTest::jsSameOrigin);
        RUNNER.run("nothing is loaded from another site: no URL but the SVG namespace, no fonts, no @import",
                ConsoleAssetsTest::noExternal);
        RUNNER.run("every element the script looks up by ID exists in the page, once", ConsoleAssetsTest::idsExist);
        RUNNER.run("the style sheet uses the agreed colors, frosted panels with a fallback, and a phone layout",
                ConsoleAssetsTest::cssLook);
        RUNNER.run("status colors always come with a text label in the page's legend", ConsoleAssetsTest::legendHasText);
        RUNNER.run("polling stops while the tab is hidden, and runs every 1 and 2 seconds", ConsoleAssetsTest::polling);
        RUNNER.run("uploads send the file's own name, and names are shown as text, isolated, and used for downloads",
                ConsoleAssetsTest::fileNames);
        RUNNER.finish();
    }

    private static void present(Path tempDir) throws IOException {
        for (String name : List.of("index.html", "console.css", "console.js")) {
            byte[] bytes = bytes(name);
            check(bytes.length > 0 && bytes.length <= ConsoleAssets.MAX_BYTES, name + " is " + bytes.length + " bytes");
            check(new String(bytes, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8).length == bytes.length,
                    name + " is not valid UTF-8");
            check(!(bytes.length >= 3 && (bytes[0] & 0xFF) == 0xEF && (bytes[1] & 0xFF) == 0xBB && (bytes[2] & 0xFF) == 0xBF),
                    name + " starts with a byte order mark");
        }
        NodeTestKit.Lines err = new NodeTestKit.Lines();
        check(ConsoleAssets.load(err.stream()).available(), "ConsoleAssets did not load the files");
        checkEquals("", err.text(), "warnings");
    }

    private static void htmlLoads(Path tempDir) throws IOException {
        String html = text("index.html");
        List<String> scripts = matches(html, "<script\\b[^>]*>");
        checkEquals(List.of("<script src=\"/console/console.js\" defer>"), scripts, "script elements");
        checkContains(html, "<script src=\"/console/console.js\" defer></script>");
        List<String> links = matches(html, "<link\\b[^>]*>");
        checkEquals(List.of("<link rel=\"stylesheet\" href=\"/console/console.css\">"), links, "link elements");
        checkContains(html, "<meta charset=\"utf-8\">");
        checkContains(html, "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
        check(html.startsWith("<!doctype html>"), "the page does not start with <!doctype html>");
    }

    private static void htmlNoInline(Path tempDir) throws IOException {
        String html = text("index.html").toLowerCase(Locale.ROOT);
        check(!html.contains("<style"), "a <style> element");
        check(!Pattern.compile("\\sstyle\\s*=").matcher(html).find(), "a style attribute");
        Matcher handler = Pattern.compile("\\son[a-z]+\\s*=").matcher(html);
        check(!handler.find(), "an event-handler attribute: " + (handler.hitEnd() ? "" : html.substring(handler.start(),
                Math.min(html.length(), handler.end() + 20))));
        check(!html.contains("javascript:"), "a javascript: URL");
        check(!html.contains("<iframe") && !html.contains("<object") && !html.contains("<embed") && !html.contains("<form"),
                "an iframe, object, embed or form");
        check(!html.contains("<base"), "a <base> element");
    }

    private static void jsNoHtmlSinks(Path tempDir) throws IOException {
        String js = code(text("console.js"));
        for (String forbidden : List.of("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "createContextualFragment",
                "DOMParser", "srcdoc", "eval(", "new Function", "Function(", "setInterval(", "importScripts", "execCommand")) {
            check(!js.contains(forbidden), "console.js contains " + forbidden);
        }
        check(!Pattern.compile("setTimeout\\(\\s*['\"`]").matcher(js).find(), "setTimeout with a string");
        checkContains(js, ".textContent = ");
    }

    private static void jsNoStyles(Path tempDir) throws IOException {
        String js = code(text("console.js"));
        check(!Pattern.compile("\\.style\\b").matcher(js).find(), "console.js touches .style");
        check(!js.contains("cssText"), "console.js sets cssText");
        check(!js.contains("CSSStyleSheet") && !js.contains("insertRule") && !js.contains("adoptedStyleSheets"),
                "console.js builds a style sheet");
        // Every setAttribute call goes through setAttr, which checks the name against the list.
        List<String> calls = matches(js, "\\.setAttribute\\(");
        checkEquals(1, calls.size(), "setAttribute calls (only the one inside setAttr)");
        checkContains(js, "if (ATTRIBUTES.indexOf(name) < 0 && name.indexOf('data-') !== 0) {");
    }

    private static void jsAttributeList(Path tempDir) throws IOException {
        String js = text("console.js");
        Matcher list = Pattern.compile("var ATTRIBUTES = \\[([^\\]]*)\\];").matcher(js);
        check(list.find(), "no ATTRIBUTES list");
        List<String> names = matches(list.group(1), "'([^']*)'", 1);
        check(names.size() >= 10, "the list is suspiciously short: " + names);
        for (String name : names) {
            String n = name.toLowerCase(Locale.ROOT);
            check(!n.equals("style") && !n.startsWith("on") && !n.equals("src") && !n.equals("srcdoc") && !n.equals("action")
                    && !n.equals("formaction") && !n.equals("xlink:href"), "the list allows \"" + name + "\"");
        }
    }

    private static void jsSameOrigin(Path tempDir) throws IOException {
        String js = text("console.js");
        checkContains(js, "credentials: 'same-origin'");
        check(!js.contains("'include'") && !js.contains("\"include\""), "credentials: include");
        check(!js.contains("mode: 'no-cors'") && !js.contains("mode: 'cors'"), "a cross-origin request mode");
        for (String path : List.of("'/cluster/status'", "'/files'", "'/cluster/events'", "'/cluster/repair'", "'/placement'")) {
            checkContains(js, path);
        }
        check(!js.contains("WebSocket") && !js.contains("EventSource") && !js.contains("sendBeacon"), "another channel");
        check(!js.contains("localStorage") && !js.contains("sessionStorage") && !js.contains("document.cookie"),
                "browser storage or cookies");
    }

    private static void noExternal(Path tempDir) throws IOException {
        for (String name : List.of("index.html", "console.css", "console.js")) {
            String text = text(name);
            Matcher url = Pattern.compile("(https?:)?//[A-Za-z0-9.-]+\\.[A-Za-z]{2,}").matcher(text.replace(SVG_NAMESPACE, ""));
            check(!url.find(), name + " names another site: " + (url.hitEnd() ? "" : url.group()));
        }
        String css = text("console.css").toLowerCase(Locale.ROOT);
        check(!css.contains("@import"), "@import");
        check(!css.contains("@font-face"), "@font-face");
        check(!css.contains("url("), "url(");
        check(!css.contains("expression("), "expression(");
    }

    private static void idsExist(Path tempDir) throws IOException {
        String html = text("index.html");
        Set<String> ids = new LinkedHashSet<>(matches(text("console.js"), "byId\\('([a-z-]+)'\\)", 1));
        check(ids.size() >= 15, "only " + ids.size() + " IDs looked up");
        for (String id : ids) {
            int count = matches(html, "id=\"" + Pattern.quote(id) + "\"").size();
            checkEquals(1, count, "elements with id=\"" + id + "\"");
        }
    }

    private static void cssLook(Path tempDir) throws IOException {
        String css = text("console.css");
        for (String color : List.of("#ECEEEA", "#1E9E5A", "#11653A", "#C93A3A")) {
            checkContains(css, color);
        }
        checkContains(css, "-webkit-backdrop-filter: blur(");
        checkContains(css, "backdrop-filter: blur(");
        checkContains(css, "@supports not ((backdrop-filter: blur(1px)) or (-webkit-backdrop-filter: blur(1px)))");
        checkContains(css, "@media (prefers-reduced-transparency: reduce)");
        checkContains(css, "@media (prefers-reduced-motion: reduce)");
        checkContains(css, "@media (max-width: 900px)");
        checkContains(css, "--font: system-ui");
        checkContains(css, "--mono: ui-monospace");
        checkContains(css, ".map-scroll { width: 100%; max-width: 100%; overflow-x: auto;");
    }

    private static void legendHasText(Path tempDir) throws IOException {
        String html = text("index.html");
        for (String label : List.of("copy", "new (repair)", "unreachable", "extra")) {
            checkContains(html, "aria-hidden=\"true\"></span>" + label + "</li>");
        }
        String js = text("console.js");
        // Every tile carries its state as words (visible and for screen readers), never color alone.
        checkContains(js, "copy: 'copy', 'new': 'new', unreachable: 'unreachable', extra: 'extra'");
        checkContains(js, "el('span', { cls: 'visually-hidden', text: label })");
        checkContains(js, "el('span', { cls: 'pill ' + (up ? 'is-up' : 'is-down'), text: node.state })");
    }

    private static void polling(Path tempDir) throws IOException {
        String js = text("console.js");
        checkContains(js, "var STATUS_MS = 1000;");
        checkContains(js, "var DATA_MS = 2000;");
        checkContains(js, "document.addEventListener('visibilitychange'");
        checkContains(js, "if (document.hidden) {\n        return;\n      }");
        checkContains(js, "statusPoller.stop();");
        checkContains(js, "dataPoller.stop();");
    }

    private static void fileNames(Path tempDir) throws IOException {
        String js = text("console.js");
        // The name goes out exactly as the browser has it, percent-encoded as UTF-8 ...
        checkContains(js, "encodedName = encodeURIComponent(file.name);");
        checkContains(js, "xhr.setRequestHeader('X-File-Name', encodedName);");
        // ... comes back on the page as text, in a bidi-isolated element ...
        checkContains(js, "el('bdi', { cls: 'file-name', text: displayName(file.name)");
        checkContains(js, "function displayName(name) {");
        checkContains(text("console.css"), ".file-name { display: block;");
        checkContains(text("console.css"), "unicode-bidi: isolate;");
        // ... and is the downloaded file's name.
        checkContains(js, "download: named ? file.name : file.fileId + '.bin'");
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static byte[] bytes(String name) throws IOException {
        try (InputStream in = ConsoleAssets.class.getResourceAsStream(ConsoleAssets.FOLDER + name)) {
            check(in != null, name + " is not on the class path (is src\\main\\resources on it?)");
            return in.readAllBytes();
        }
    }

    private static String text(String name) throws IOException {
        return new String(bytes(name), StandardCharsets.UTF_8);
    }

    /** The script without its comments, so that the rules written in comments do not count as code. */
    private static String code(String js) {
        return js.replaceAll("(?m)^\\s*//.*$", "");
    }

    private static List<String> matches(String text, String regex) {
        return matches(text, regex, 0);
    }

    private static List<String> matches(String text, String regex, int group) {
        List<String> found = new ArrayList<>();
        Matcher m = Pattern.compile(regex).matcher(text);
        while (m.find()) {
            found.add(m.group(group));
        }
        return found;
    }
}
