package ru.refiq.strategy.clickstream;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

public final class ClickstreamAddresses {

    private static final Pattern IPV4 = Pattern.compile("^(?:\\d{1,3}\\.){3}\\d{1,3}$");
    private static final Pattern IPV6 = Pattern.compile("^[0-9a-fA-F:.]+$");
    private static final Inet6Address UNSPECIFIED = parse("::");

    private ClickstreamAddresses() {
    }

    static Inet6Address inet6(String value) {
        String canonical = canonicalOrUnspecified(value);
        return "::".equals(canonical) ? UNSPECIFIED : parse(canonical);
    }

    public static String canonicalOrUnspecified(String value) {
        if (value == null || value.isBlank() || !literal(value.trim())) {
            return "::";
        }
        try {
            InetAddress address = InetAddress.getByName(value.trim());
            if (address instanceof Inet4Address) {
                return value.trim();
            }
            return address.getHostAddress();
        } catch (UnknownHostException e) {
            return "::";
        }
    }

    private static boolean literal(String value) {
        if (value.indexOf(':') >= 0) {
            return IPV6.matcher(value).matches();
        }
        if (!IPV4.matcher(value).matches()) {
            return false;
        }
        for (String octet : value.split("\\.")) {
            int parsed = Integer.parseInt(octet);
            if (parsed > 255) {
                return false;
            }
        }
        return true;
    }

    private static Inet6Address parse(String value) {
        try {
            return (Inet6Address) InetAddress.getByName(value);
        } catch (UnknownHostException | ClassCastException e) {
            throw new IllegalStateException("unspecified ipv6 is unavailable", e);
        }
    }
}
