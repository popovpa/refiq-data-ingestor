package ru.refiq.strategy.clickstream;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.regex.Pattern;

public final class ClickstreamAddresses {

    private static final Pattern IPV4 = Pattern.compile("^(?:\\d{1,3}\\.){3}\\d{1,3}$");
    private static final Pattern IPV6 = Pattern.compile("^[0-9a-fA-F:.]+$");
    private static final byte[] UNSPECIFIED_BYTES = new byte[16];
    private static final Inet6Address UNSPECIFIED = ipv6(UNSPECIFIED_BYTES);

    private ClickstreamAddresses() {
    }

    static Inet6Address inet6(String value) {
        String canonical = canonicalOrUnspecified(value);
        if ("::".equals(canonical)) {
            return UNSPECIFIED;
        }
        try {
            InetAddress address = InetAddress.getByName(canonical);
            if (address instanceof Inet4Address ipv4) {
                byte[] mapped = new byte[16];
                mapped[10] = (byte) 0xff;
                mapped[11] = (byte) 0xff;
                System.arraycopy(ipv4.getAddress(), 0, mapped, 12, 4);
                return ipv6(mapped);
            }
            byte[] raw = address.getAddress();
            if (raw.length != 16) {
                return UNSPECIFIED;
            }
            return ipv6(Arrays.copyOf(raw, 16));
        } catch (UnknownHostException e) {
            return UNSPECIFIED;
        }
    }

    private static Inet6Address ipv6(byte[] bytes) {
        try {
            return Inet6Address.getByAddress(null, bytes, -1);
        } catch (UnknownHostException e) {
            throw new IllegalStateException("unspecified ipv6 is unavailable", e);
        }
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

}
