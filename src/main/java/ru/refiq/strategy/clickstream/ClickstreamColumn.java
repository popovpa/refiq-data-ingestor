package ru.refiq.strategy.clickstream;

import java.util.Arrays;
import java.util.List;

/**
 * Column order of {@code clickstream_events}.
 * The list is the single insert mapping: names, wire types and defaults.
 */
public enum ClickstreamColumn {
    EVENT_ID("event_id", "Int64", "0", true, true, null, null, "event_id", "eventId"),
    CLIENT_EVENT_ID("client_event_id", "Int64", "0", false, true, null, null, "client_event_id", "clientEventId"),
    BATCH_ID("batch_id", "Int64", "0", false, false, null, null, "batch_id", "batchId"),
    SCHEMA_VERSION("schema_version", "UInt16", "1", false, false, null, null, "schema_version", "schemaVersion"),
    EVENT_TYPE("event_type", "LowCardinality(String)", "''", false, false, null, null, "event_type", "eventType", "type", "event"),
    CUSTOM_EVENT_NAME("custom_event_name", "String", "''", false, false, null, null, "custom_event_name", "customEventName"),
    SEQUENCE("sequence", "UInt32", "0", false, false, null, null, "sequence"),
    OCCURRED_TS("occurred_ts", "Int64", "0", false, false, null, null, "occurred_ts", "occurredTs", "occurredAtMs"),
    SENT_TS("sent_ts", "Int64", "0", false, false, null, null, "sent_ts", "sentTs", "sentAtMs"),
    CAPTURED_TS("captured_ts", "Int64", null, true, true, null, null, "captured_ts", "capturedTs"),
    VISITOR_ID("visitor_id", "Int64", "0", false, false, null, null, "visitor_id", "visitorId"),
    SESSION_ID("session_id", "Int64", "0", false, false, null, null, "session_id", "sessionId"),
    PAGE_VIEW_ID("page_view_id", "Int64", "0", false, false, null, null, "page_view_id", "pageViewId"),
    PREVIOUS_PAGE_VIEW_ID("previous_page_view_id", "Int64", "0", false, false, null, null, "previous_page_view_id", "previousPageViewId"),
    SITE_KEY("site_key", "LowCardinality(String)", "''", false, false, null, null, "site_key", "siteKey", "script_id", "scriptId"),
    SITE_ID("site_id", "Int64", "0", true, true, null, null, "site_id", "siteId"),
    BUSINESS_ID("business_id", "Int64", "0", true, true, null, null, "business_id", "businessId"),
    PARTNER_ID("partner_id", "Int64", "0", true, true, null, null, "partner_id", "partnerId"),
    OFFER_ID("offer_id", "Int64", "0", true, true, null, null, "offer_id", "offerId"),
    CAMPAIGN_ID("campaign_id", "Int64", "0", true, true, null, null, "campaign_id", "campaignId"),
    TRACKING_LINK_ID("tracking_link_id", "Int64", "0", true, true, null, null, "tracking_link_id", "trackingLinkId"),
    TRAFFIC_OWNERSHIP("traffic_ownership", "LowCardinality(String)", "''", true, true, null, null, "traffic_ownership", "trafficOwnership"),
    RQCID("rqcid", "String", "''", false, false, null, null, "rqcid"),
    RQCID_CAPTURED_TS("rqcid_captured_ts", "Int64", "0", false, false, null, null, "rqcid_captured_ts", "rqcidCapturedTs"),
    UTM_SOURCE("utm_source", "LowCardinality(String)", "''", false, false, null, null, "utm_source", "utmSource"),
    UTM_MEDIUM("utm_medium", "LowCardinality(String)", "''", false, false, null, null, "utm_medium", "utmMedium"),
    UTM_CAMPAIGN("utm_campaign", "String", "''", false, false, null, null, "utm_campaign", "utmCampaign"),
    UTM_CONTENT("utm_content", "String", "''", false, false, null, null, "utm_content", "utmContent"),
    UTM_TERM("utm_term", "String", "''", false, false, null, null, "utm_term", "utmTerm"),
    YCLID("yclid", "String", "''", false, false, null, null, "yclid"),
    GCLID("gclid", "String", "''", false, false, null, null, "gclid"),
    FBCLID("fbclid", "String", "''", false, false, null, null, "fbclid"),
    TTCLID("ttclid", "String", "''", false, false, null, null, "ttclid"),
    VK_CLICK_ID("vk_click_id", "String", "''", false, false, null, null, "vk_click_id", "vkClickId"),
    LANDING_ORIGIN("landing_origin", "String", "''", false, false, "landing", "origin", "landing_origin", "landingOrigin"),
    LANDING_HOSTNAME("landing_hostname", "LowCardinality(String)", "''", false, false, "landing", "hostname", "landing_hostname", "landingHostname"),
    LANDING_PATHNAME("landing_pathname", "String", "''", false, false, "landing", "pathname", "landing_pathname", "landingPathname"),
    LANDING_REFERRER_ORIGIN("landing_referrer_origin", "String", "''", false, false, "landing", "referrer_origin", "landing_referrer_origin", "landingReferrerOrigin"),
    LANDING_REFERRER_HOSTNAME("landing_referrer_hostname", "LowCardinality(String)", "''", false, false, "landing", "referrer_hostname", "landing_referrer_hostname", "landingReferrerHostname"),
    LANDING_REFERRER_PATHNAME("landing_referrer_pathname", "String", "''", false, false, "landing", "referrer_pathname", "landing_referrer_pathname", "landingReferrerPathname"),
    PAGE_PROTOCOL("page_protocol", "LowCardinality(String)", "''", false, false, "page", "protocol", "page_protocol", "pageProtocol"),
    PAGE_ORIGIN("page_origin", "String", "''", false, false, "page", "origin", "page_origin", "pageOrigin"),
    PAGE_HOSTNAME("page_hostname", "LowCardinality(String)", "''", false, false, "page", "hostname", "page_hostname", "pageHostname"),
    PAGE_PORT("page_port", "UInt16", "0", false, false, "page", "port", "page_port", "pagePort"),
    PAGE_PATHNAME("page_pathname", "String", "''", false, false, "page", "pathname", "page_pathname", "pagePathname"),
    PAGE_HASH_ROUTE("page_hash_route", "String", "''", false, false, "page", "hash_route", "page_hash_route", "pageHashRoute"),
    PAGE_TITLE("page_title", "String", "''", false, false, "page", "title", "page_title", "pageTitle"),
    PAGE_CONTENT_TYPE("page_content_type", "LowCardinality(String)", "''", false, false, "page", "content_type", "page_content_type", "pageContentType"),
    PAGE_CHARSET("page_charset", "LowCardinality(String)", "''", false, false, "page", "charset", "page_charset", "pageCharset"),
    PAGE_NAVIGATION_TYPE("page_navigation_type", "LowCardinality(String)", "''", false, false, "page", "navigation_type", "page_navigation_type", "pageNavigationType"),
    PAGE_HISTORY_LENGTH("page_history_length", "UInt16", "0", false, false, "page", "history_length", "page_history_length", "pageHistoryLength"),
    PAGE_IS_TOP_FRAME("page_is_top_frame", "Int8", "-1", false, false, "page", "is_top_frame", "page_is_top_frame", "pageIsTopFrame"),
    PAGE_READY_STATE("page_ready_state", "LowCardinality(String)", "''", false, false, "page", "ready_state", "page_ready_state", "pageReadyState"),
    PAGE_VISIBILITY_STATE("page_visibility_state", "LowCardinality(String)", "''", false, false, "page", "visibility_state", "page_visibility_state", "pageVisibilityState"),
    PAGE_DOCUMENT_WIDTH("page_document_width", "UInt32", "0", false, false, "page", "document_width", "page_document_width", "pageDocumentWidth"),
    PAGE_DOCUMENT_HEIGHT("page_document_height", "UInt32", "0", false, false, "page", "document_height", "page_document_height", "pageDocumentHeight"),
    PAGE_REFERRER_ORIGIN("page_referrer_origin", "String", "''", false, false, "page", "referrer_origin", "page_referrer_origin", "pageReferrerOrigin"),
    PAGE_REFERRER_HOSTNAME("page_referrer_hostname", "LowCardinality(String)", "''", false, false, "page", "referrer_hostname", "page_referrer_hostname", "pageReferrerHostname"),
    PAGE_REFERRER_PATHNAME("page_referrer_pathname", "String", "''", false, false, "page", "referrer_pathname", "page_referrer_pathname", "pageReferrerPathname"),
    NAVIGATOR_USER_AGENT("navigator_user_agent", "String", "''", false, false, "navigator", "user_agent", "navigator_user_agent", "navigatorUserAgent"),
    NAVIGATOR_LANGUAGE("navigator_language", "LowCardinality(String)", "''", false, false, "navigator", "language", "navigator_language", "navigatorLanguage"),
    NAVIGATOR_LANGUAGES("navigator_languages", "String", "''", false, false, "navigator", "languages", "navigator_languages", "navigatorLanguages"),
    NAVIGATOR_PLATFORM("navigator_platform", "LowCardinality(String)", "''", false, false, "navigator", "platform", "navigator_platform", "navigatorPlatform"),
    NAVIGATOR_VENDOR("navigator_vendor", "LowCardinality(String)", "''", false, false, "navigator", "vendor", "navigator_vendor", "navigatorVendor"),
    NAVIGATOR_COOKIE_ENABLED("navigator_cookie_enabled", "Int8", "-1", false, false, "navigator", "cookie_enabled", "navigator_cookie_enabled", "navigatorCookieEnabled"),
    NAVIGATOR_WEBDRIVER("navigator_webdriver", "Int8", "-1", false, false, "navigator", "webdriver", "navigator_webdriver", "navigatorWebdriver"),
    NAVIGATOR_PDF_VIEWER_ENABLED("navigator_pdf_viewer_enabled", "Int8", "-1", false, false, "navigator", "pdf_viewer_enabled", "navigator_pdf_viewer_enabled", "navigatorPdfViewerEnabled"),
    NAVIGATOR_HARDWARE_CONCURRENCY("navigator_hardware_concurrency", "UInt16", "0", false, false, "navigator", "hardware_concurrency", "navigator_hardware_concurrency", "navigatorHardwareConcurrency"),
    NAVIGATOR_DEVICE_MEMORY_GB("navigator_device_memory_gb", "Float32", "-1", false, false, "navigator", "device_memory_gb", "navigator_device_memory_gb", "navigatorDeviceMemoryGb"),
    NAVIGATOR_MAX_TOUCH_POINTS("navigator_max_touch_points", "UInt16", "0", false, false, "navigator", "max_touch_points", "navigator_max_touch_points", "navigatorMaxTouchPoints"),
    NAVIGATOR_UA_MOBILE("navigator_ua_mobile", "Int8", "-1", false, false, "navigator", "ua_mobile", "navigator_ua_mobile", "navigatorUaMobile"),
    NAVIGATOR_UA_PLATFORM("navigator_ua_platform", "LowCardinality(String)", "''", false, false, "navigator", "ua_platform", "navigator_ua_platform", "navigatorUaPlatform"),
    NAVIGATOR_UA_BRANDS("navigator_ua_brands", "String", "''", false, false, "navigator", "ua_brands", "navigator_ua_brands", "navigatorUaBrands"),
    SCREEN_WIDTH("screen_width", "UInt32", "0", false, false, "screen", "width", "screen_width", "screenWidth"),
    SCREEN_HEIGHT("screen_height", "UInt32", "0", false, false, "screen", "height", "screen_height", "screenHeight"),
    SCREEN_AVAIL_WIDTH("screen_avail_width", "UInt32", "0", false, false, "screen", "avail_width", "screen_avail_width", "screenAvailWidth"),
    SCREEN_AVAIL_HEIGHT("screen_avail_height", "UInt32", "0", false, false, "screen", "avail_height", "screen_avail_height", "screenAvailHeight"),
    SCREEN_COLOR_DEPTH("screen_color_depth", "UInt16", "0", false, false, "screen", "color_depth", "screen_color_depth", "screenColorDepth"),
    SCREEN_PIXEL_DEPTH("screen_pixel_depth", "UInt16", "0", false, false, "screen", "pixel_depth", "screen_pixel_depth", "screenPixelDepth"),
    SCREEN_ORIENTATION_TYPE("screen_orientation_type", "LowCardinality(String)", "''", false, false, "screen", "orientation_type", "screen_orientation_type", "screenOrientationType"),
    SCREEN_ORIENTATION_ANGLE("screen_orientation_angle", "Int16", "0", false, false, "screen", "orientation_angle", "screen_orientation_angle", "screenOrientationAngle"),
    SCREEN_DEVICE_PIXEL_RATIO("screen_device_pixel_ratio", "Float32", "-1", false, false, "screen", "device_pixel_ratio", "screen_device_pixel_ratio", "screenDevicePixelRatio"),
    VIEWPORT_WIDTH("viewport_width", "UInt32", "0", false, false, "viewport", "width", "viewport_width", "viewportWidth"),
    VIEWPORT_HEIGHT("viewport_height", "UInt32", "0", false, false, "viewport", "height", "viewport_height", "viewportHeight"),
    WINDOW_OUTER_WIDTH("window_outer_width", "UInt32", "0", false, false, "window", "outer_width", "window_outer_width", "windowOuterWidth"),
    WINDOW_OUTER_HEIGHT("window_outer_height", "UInt32", "0", false, false, "window", "outer_height", "window_outer_height", "windowOuterHeight"),
    VISUAL_VIEWPORT_WIDTH("visual_viewport_width", "Float32", "-1", false, false, "visualViewport", "width", "visual_viewport_width", "visualViewportWidth"),
    VISUAL_VIEWPORT_HEIGHT("visual_viewport_height", "Float32", "-1", false, false, "visualViewport", "height", "visual_viewport_height", "visualViewportHeight"),
    VISUAL_VIEWPORT_SCALE("visual_viewport_scale", "Float32", "-1", false, false, "visualViewport", "scale", "visual_viewport_scale", "visualViewportScale"),
    VISUAL_VIEWPORT_OFFSET_LEFT("visual_viewport_offset_left", "Float32", "-1", false, false, "visualViewport", "offset_left", "visual_viewport_offset_left", "visualViewportOffsetLeft"),
    VISUAL_VIEWPORT_OFFSET_TOP("visual_viewport_offset_top", "Float32", "-1", false, false, "visualViewport", "offset_top", "visual_viewport_offset_top", "visualViewportOffsetTop"),
    MEDIA_COLOR_SCHEME("media_color_scheme", "LowCardinality(String)", "''", false, false, "mediaFeatures", "color_scheme", "media_color_scheme", "mediaColorScheme"),
    MEDIA_REDUCED_MOTION("media_reduced_motion", "Int8", "-1", false, false, "mediaFeatures", "reduced_motion", "media_reduced_motion", "mediaReducedMotion"),
    MEDIA_PREFERRED_CONTRAST("media_preferred_contrast", "LowCardinality(String)", "''", false, false, "mediaFeatures", "preferred_contrast", "media_preferred_contrast", "mediaPreferredContrast"),
    MEDIA_POINTER("media_pointer", "LowCardinality(String)", "''", false, false, "mediaFeatures", "pointer", "media_pointer", "mediaPointer"),
    MEDIA_ANY_POINTER("media_any_pointer", "LowCardinality(String)", "''", false, false, "mediaFeatures", "any_pointer", "media_any_pointer", "mediaAnyPointer"),
    MEDIA_HOVER("media_hover", "Int8", "-1", false, false, "mediaFeatures", "hover", "media_hover", "mediaHover"),
    MEDIA_ANY_HOVER("media_any_hover", "Int8", "-1", false, false, "mediaFeatures", "any_hover", "media_any_hover", "mediaAnyHover"),
    MEDIA_DISPLAY_MODE("media_display_mode", "LowCardinality(String)", "''", false, false, "mediaFeatures", "display_mode", "media_display_mode", "mediaDisplayMode"),
    LOCALE("locale", "LowCardinality(String)", "''", false, false, null, null, "locale"),
    TIMEZONE("timezone", "LowCardinality(String)", "''", false, false, null, null, "timezone"),
    TIMEZONE_OFFSET_MINUTES("timezone_offset_minutes", "Int16", "0", false, false, null, null, "timezone_offset_minutes", "timezoneOffsetMinutes"),
    CALENDAR("calendar", "LowCardinality(String)", "''", false, false, null, null, "calendar"),
    NUMBERING_SYSTEM("numbering_system", "LowCardinality(String)", "''", false, false, null, null, "numbering_system", "numberingSystem"),
    NETWORK_ONLINE("network_online", "Int8", "-1", false, false, "network", "online", "network_online", "networkOnline"),
    NETWORK_TYPE("network_type", "LowCardinality(String)", "''", false, false, "network", "type", "network_type", "networkType"),
    NETWORK_EFFECTIVE_TYPE("network_effective_type", "LowCardinality(String)", "''", false, false, "network", "effective_type", "network_effective_type", "networkEffectiveType"),
    NETWORK_DOWNLINK_MBPS("network_downlink_mbps", "Float32", "-1", false, false, "network", "downlink_mbps", "network_downlink_mbps", "networkDownlinkMbps"),
    NETWORK_RTT("network_rtt", "Int32", "-1", false, false, "network", "rtt", "network_rtt", "networkRtt"),
    NETWORK_SAVE_DATA("network_save_data", "Int8", "-1", false, false, "network", "save_data", "network_save_data", "networkSaveData"),
    PERFORMANCE_TIME_ORIGIN_TS("performance_time_origin_ts", "Int64", "0", false, false, "performance", "time_origin_ts", "performance_time_origin_ts", "performanceTimeOriginTs"),
    PERFORMANCE_REDIRECT_COUNT("performance_redirect_count", "UInt16", "0", false, false, "performance", "redirect_count", "performance_redirect_count", "performanceRedirectCount"),
    PERFORMANCE_REDIRECT("performance_redirect", "Int64", "-1", false, false, "performance", "redirect", "performance_redirect", "performanceRedirect"),
    PERFORMANCE_DNS("performance_dns", "Int64", "-1", false, false, "performance", "dns", "performance_dns", "performanceDns"),
    PERFORMANCE_TCP("performance_tcp", "Int64", "-1", false, false, "performance", "tcp", "performance_tcp", "performanceTcp"),
    PERFORMANCE_TLS("performance_tls", "Int64", "-1", false, false, "performance", "tls", "performance_tls", "performanceTls"),
    PERFORMANCE_TTFB("performance_ttfb", "Int64", "-1", false, false, "performance", "ttfb", "performance_ttfb", "performanceTtfb"),
    PERFORMANCE_RESPONSE("performance_response", "Int64", "-1", false, false, "performance", "response", "performance_response", "performanceResponse"),
    PERFORMANCE_DOM_INTERACTIVE("performance_dom_interactive", "Int64", "-1", false, false, "performance", "dom_interactive", "performance_dom_interactive", "performanceDomInteractive"),
    PERFORMANCE_DOM_CONTENT_LOADED("performance_dom_content_loaded", "Int64", "-1", false, false, "performance", "dom_content_loaded", "performance_dom_content_loaded", "performanceDomContentLoaded"),
    PERFORMANCE_LOAD("performance_load", "Int64", "-1", false, false, "performance", "load", "performance_load", "performanceLoad"),
    PERFORMANCE_NEXT_HOP_PROTOCOL("performance_next_hop_protocol", "LowCardinality(String)", "''", false, false, "performance", "next_hop_protocol", "performance_next_hop_protocol", "performanceNextHopProtocol"),
    PERFORMANCE_TRANSFER_SIZE("performance_transfer_size", "UInt64", "0", false, false, "performance", "transfer_size", "performance_transfer_size", "performanceTransferSize"),
    PERFORMANCE_ENCODED_BODY_SIZE("performance_encoded_body_size", "UInt64", "0", false, false, "performance", "encoded_body_size", "performance_encoded_body_size", "performanceEncodedBodySize"),
    PERFORMANCE_DECODED_BODY_SIZE("performance_decoded_body_size", "UInt64", "0", false, false, "performance", "decoded_body_size", "performance_decoded_body_size", "performanceDecodedBodySize"),
    WEB_VITAL_FP("web_vital_fp", "Int64", "-1", false, false, "webVitals", "fp", "web_vital_fp", "webVitalFp"),
    WEB_VITAL_FCP("web_vital_fcp", "Int64", "-1", false, false, "webVitals", "fcp", "web_vital_fcp", "webVitalFcp"),
    WEB_VITAL_LCP("web_vital_lcp", "Int64", "-1", false, false, "webVitals", "lcp", "web_vital_lcp", "webVitalLcp"),
    WEB_VITAL_CLS("web_vital_cls", "Float32", "-1", false, false, "webVitals", "cls", "web_vital_cls", "webVitalCls"),
    WEB_VITAL_INP("web_vital_inp", "Int64", "-1", false, false, "webVitals", "inp", "web_vital_inp", "webVitalInp"),
    RESOURCE_COUNT("resource_count", "UInt32", "0", false, false, "resources", "count", "resource_count", "resourceCount"),
    RESOURCE_SCRIPT_COUNT("resource_script_count", "UInt32", "0", false, false, "resources", "script_count", "resource_script_count", "resourceScriptCount"),
    RESOURCE_IMAGE_COUNT("resource_image_count", "UInt32", "0", false, false, "resources", "image_count", "resource_image_count", "resourceImageCount"),
    RESOURCE_XHR_FETCH_COUNT("resource_xhr_fetch_count", "UInt32", "0", false, false, "resources", "xhr_fetch_count", "resource_xhr_fetch_count", "resourceXhrFetchCount"),
    RESOURCE_STYLESHEET_COUNT("resource_stylesheet_count", "UInt32", "0", false, false, "resources", "stylesheet_count", "resource_stylesheet_count", "resourceStylesheetCount"),
    RESOURCE_FONT_COUNT("resource_font_count", "UInt32", "0", false, false, "resources", "font_count", "resource_font_count", "resourceFontCount"),
    RESOURCE_MEDIA_COUNT("resource_media_count", "UInt32", "0", false, false, "resources", "media_count", "resource_media_count", "resourceMediaCount"),
    RESOURCE_FAILED_COUNT("resource_failed_count", "UInt32", "0", false, false, "resources", "failed_count", "resource_failed_count", "resourceFailedCount"),
    RESOURCE_TRANSFER_BYTES("resource_transfer_bytes", "UInt64", "0", false, false, "resources", "transfer_bytes", "resource_transfer_bytes", "resourceTransferBytes"),
    RESOURCE_SLOWEST("resource_slowest", "Int64", "-1", false, false, "resources", "slowest", "resource_slowest", "resourceSlowest"),
    CLICK_CLIENT_X("click_client_x", "Int32", "-1", false, false, "click", "client_x", "click_client_x", "clickClientX"),
    CLICK_CLIENT_Y("click_client_y", "Int32", "-1", false, false, "click", "client_y", "click_client_y", "clickClientY"),
    CLICK_PAGE_X("click_page_x", "Int32", "-1", false, false, "click", "page_x", "click_page_x", "clickPageX"),
    CLICK_PAGE_Y("click_page_y", "Int32", "-1", false, false, "click", "page_y", "click_page_y", "clickPageY"),
    CLICK_RELATIVE_X("click_relative_x", "Float32", "-1", false, false, "click", "relative_x", "click_relative_x", "clickRelativeX"),
    CLICK_RELATIVE_Y("click_relative_y", "Float32", "-1", false, false, "click", "relative_y", "click_relative_y", "clickRelativeY"),
    CLICK_BUTTON("click_button", "Int8", "-1", false, false, "click", "button", "click_button", "clickButton"),
    CLICK_POINTER_TYPE("click_pointer_type", "LowCardinality(String)", "''", false, false, "click", "pointer_type", "click_pointer_type", "clickPointerType"),
    CLICK_CTRL_KEY("click_ctrl_key", "Int8", "-1", false, false, "click", "ctrl_key", "click_ctrl_key", "clickCtrlKey"),
    CLICK_SHIFT_KEY("click_shift_key", "Int8", "-1", false, false, "click", "shift_key", "click_shift_key", "clickShiftKey"),
    CLICK_ALT_KEY("click_alt_key", "Int8", "-1", false, false, "click", "alt_key", "click_alt_key", "clickAltKey"),
    CLICK_META_KEY("click_meta_key", "Int8", "-1", false, false, "click", "meta_key", "click_meta_key", "clickMetaKey"),
    POINTER_PRESSURE("pointer_pressure", "Float32", "-1", false, false, "pointer", "pressure", "pointer_pressure", "pointerPressure"),
    POINTER_WIDTH("pointer_width", "Float32", "-1", false, false, "pointer", "width", "pointer_width", "pointerWidth"),
    POINTER_HEIGHT("pointer_height", "Float32", "-1", false, false, "pointer", "height", "pointer_height", "pointerHeight"),
    POINTER_IS_PRIMARY("pointer_is_primary", "Int8", "-1", false, false, "pointer", "is_primary", "pointer_is_primary", "pointerIsPrimary"),
    TARGET_TRACKING_ID("target_tracking_id", "String", "''", false, false, "target", "tracking_id", "target_tracking_id", "targetTrackingId"),
    TARGET_ELEMENT_ID("target_element_id", "String", "''", false, false, "target", "element_id", "target_element_id", "targetElementId"),
    TARGET_TAG("target_tag", "LowCardinality(String)", "''", false, false, "target", "tag", "target_tag", "targetTag"),
    TARGET_ROLE("target_role", "LowCardinality(String)", "''", false, false, "target", "role", "target_role", "targetRole"),
    TARGET_HREF_ORIGIN("target_href_origin", "String", "''", false, false, "target", "href_origin", "target_href_origin", "targetHrefOrigin"),
    TARGET_HREF_HOSTNAME("target_href_hostname", "LowCardinality(String)", "''", false, false, "target", "href_hostname", "target_href_hostname", "targetHrefHostname"),
    TARGET_HREF_PATHNAME("target_href_pathname", "String", "''", false, false, "target", "href_pathname", "target_href_pathname", "targetHrefPathname"),
    TARGET_IS_EXTERNAL("target_is_external", "Int8", "-1", false, false, "target", "is_external", "target_is_external", "targetIsExternal"),
    TARGET_WIDTH("target_width", "Float32", "-1", false, false, "target", "width", "target_width", "targetWidth"),
    TARGET_HEIGHT("target_height", "Float32", "-1", false, false, "target", "height", "target_height", "targetHeight"),
    SCROLL_X("scroll_x", "Int32", "-1", false, false, "scroll", "x", "scroll_x", "scrollX"),
    SCROLL_Y("scroll_y", "Int32", "-1", false, false, "scroll", "y", "scroll_y", "scrollY"),
    SCROLL_DOCUMENT_HEIGHT("scroll_document_height", "UInt32", "0", false, false, "scroll", "document_height", "scroll_document_height", "scrollDocumentHeight"),
    SCROLL_VIEWPORT_HEIGHT("scroll_viewport_height", "UInt32", "0", false, false, "scroll", "viewport_height", "scroll_viewport_height", "scrollViewportHeight"),
    SCROLL_DEPTH("scroll_depth", "Float32", "-1", false, false, "scroll", "depth", "scroll_depth", "scrollDepth"),
    SCROLL_MAX_DEPTH("scroll_max_depth", "Float32", "-1", false, false, "scroll", "max_depth", "scroll_max_depth", "scrollMaxDepth"),
    SCROLL_DIRECTION("scroll_direction", "LowCardinality(String)", "''", false, false, "scroll", "direction", "scroll_direction", "scrollDirection"),
    SCROLL_MILESTONE("scroll_milestone", "UInt8", "0", false, false, "scroll", "milestone", "scroll_milestone", "scrollMilestone"),
    SCROLL_TIME_TO_25("scroll_time_to_25", "Int64", "-1", false, false, "scroll", "time_to_25", "scroll_time_to_25", "scrollTimeTo25"),
    SCROLL_TIME_TO_50("scroll_time_to_50", "Int64", "-1", false, false, "scroll", "time_to_50", "scroll_time_to_50", "scrollTimeTo50"),
    SCROLL_TIME_TO_75("scroll_time_to_75", "Int64", "-1", false, false, "scroll", "time_to_75", "scroll_time_to_75", "scrollTimeTo75"),
    SCROLL_TIME_TO_90("scroll_time_to_90", "Int64", "-1", false, false, "scroll", "time_to_90", "scroll_time_to_90", "scrollTimeTo90"),
    SCROLL_TIME_TO_100("scroll_time_to_100", "Int64", "-1", false, false, "scroll", "time_to_100", "scroll_time_to_100", "scrollTimeTo100"),
    ENGAGEMENT_TOTAL_DURATION("engagement_total_duration", "Int64", "-1", false, false, "engagement", "total_duration", "engagement_total_duration", "engagementTotalDuration"),
    ENGAGEMENT_VISIBLE_DURATION("engagement_visible_duration", "Int64", "-1", false, false, "engagement", "visible_duration", "engagement_visible_duration", "engagementVisibleDuration"),
    ENGAGEMENT_ACTIVE_DURATION("engagement_active_duration", "Int64", "-1", false, false, "engagement", "active_duration", "engagement_active_duration", "engagementActiveDuration"),
    ENGAGEMENT_TIME_TO_FIRST_INTERACTION("engagement_time_to_first_interaction", "Int64", "-1", false, false, "engagement", "time_to_first_interaction", "engagement_time_to_first_interaction", "engagementTimeToFirstInteraction"),
    ENGAGEMENT_CLICK_COUNT("engagement_click_count", "UInt32", "0", false, false, "engagement", "click_count", "engagement_click_count", "engagementClickCount"),
    ENGAGEMENT_SCROLL_COUNT("engagement_scroll_count", "UInt32", "0", false, false, "engagement", "scroll_count", "engagement_scroll_count", "engagementScrollCount"),
    ENGAGEMENT_MAX_SCROLL_DEPTH("engagement_max_scroll_depth", "Float32", "-1", false, false, "engagement", "max_scroll_depth", "engagement_max_scroll_depth", "engagementMaxScrollDepth"),
    ENGAGEMENT_EXIT_REASON("engagement_exit_reason", "LowCardinality(String)", "''", false, false, "engagement", "exit_reason", "engagement_exit_reason", "engagementExitReason"),
    FORM_TRACKING_ID("form_tracking_id", "String", "''", false, false, "form", "tracking_id", "form_tracking_id", "formTrackingId"),
    FORM_ELEMENT_ID("form_element_id", "String", "''", false, false, "form", "element_id", "form_element_id", "formElementId"),
    FORM_FIELD_COUNT("form_field_count", "UInt16", "0", false, false, "form", "field_count", "form_field_count", "formFieldCount"),
    FORM_VALID("form_valid", "Int8", "-1", false, false, "form", "valid", "form_valid", "formValid"),
    FORM_METHOD("form_method", "LowCardinality(String)", "''", false, false, "form", "method", "form_method", "formMethod"),
    MEDIA_TRACKING_ID("media_tracking_id", "String", "''", false, false, "media", "tracking_id", "media_tracking_id", "mediaTrackingId"),
    MEDIA_TYPE("media_type", "LowCardinality(String)", "''", false, false, "media", "type", "media_type", "mediaType"),
    MEDIA_ACTION("media_action", "LowCardinality(String)", "''", false, false, "media", "action", "media_action", "mediaAction"),
    MEDIA_POSITION("media_position", "Int64", "-1", false, false, "media", "position", "media_position", "mediaPosition"),
    MEDIA_DURATION("media_duration", "Int64", "-1", false, false, "media", "duration", "media_duration", "mediaDuration"),
    MEDIA_PERCENT("media_percent", "Float32", "-1", false, false, "media", "percent", "media_percent", "mediaPercent"),
    ERROR_TYPE("error_type", "LowCardinality(String)", "''", false, false, "error", "type", "error_type", "errorType"),
    ERROR_FINGERPRINT("error_fingerprint", "UInt64", "0", false, false, "error", "fingerprint", "error_fingerprint", "errorFingerprint"),
    ERROR_SCRIPT_PATH("error_script_path", "String", "''", false, false, "error", "script_path", "error_script_path", "errorScriptPath"),
    ERROR_LINE("error_line", "UInt32", "0", false, false, "error", "line", "error_line", "errorLine"),
    ERROR_COLUMN("error_column", "UInt32", "0", false, false, "error", "column", "error_column", "errorColumn"),
    PRIVACY_GLOBAL_PRIVACY_CONTROL("privacy_global_privacy_control", "Int8", "-1", false, false, "privacy", "global_privacy_control", "privacy_global_privacy_control", "privacyGlobalPrivacyControl"),
    PRIVACY_DO_NOT_TRACK("privacy_do_not_track", "Int8", "-1", false, false, "privacy", "do_not_track", "privacy_do_not_track", "privacyDoNotTrack"),
    CONSENT_ANALYTICS("consent_analytics", "LowCardinality(String)", "''", false, false, "consent", "analytics", "consent_analytics", "consentAnalytics"),
    CONSENT_UPDATED_TS("consent_updated_ts", "Int64", "0", false, false, "consent", "updated_ts", "consent_updated_ts", "consentUpdatedTs"),
    CONSENT_POLICY_VERSION("consent_policy_version", "String", "''", false, false, "consent", "policy_version", "consent_policy_version", "consentPolicyVersion"),
    SDK_VERSION("sdk_version", "LowCardinality(String)", "''", false, false, "sdk", "version", "sdk_version", "sdkVersion"),
    SDK_INTEGRATION_MODE("sdk_integration_mode", "LowCardinality(String)", "''", false, false, "sdk", "integration_mode", "sdk_integration_mode", "sdkIntegrationMode"),
    BATCH_EVENT_COUNT("batch_event_count", "UInt16", "0", false, false, null, null, "batch_event_count", "batchEventCount"),
    BATCH_BYTES("batch_bytes", "UInt32", "0", false, false, null, null, "batch_bytes", "batchBytes"),
    DELIVERY_TRANSPORT("delivery_transport", "LowCardinality(String)", "''", false, false, "delivery", "transport", "delivery_transport", "deliveryTransport"),
    DELIVERY_ATTEMPT("delivery_attempt", "UInt16", "0", false, false, "delivery", "attempt", "delivery_attempt", "deliveryAttempt"),
    DELIVERY_FROM_INDEXEDDB("delivery_from_indexeddb", "Int8", "0", false, false, "delivery", "from_indexeddb", "delivery_from_indexeddb", "deliveryFromIndexeddb"),
    DELIVERY_FINAL_FLUSH("delivery_final_flush", "Int8", "0", false, false, "delivery", "final_flush", "delivery_final_flush", "deliveryFinalFlush"),
    REQUEST_ID("request_id", "Int64", "0", false, false, null, null, "request_id", "requestId"),
    IP_ADDRESS("ip_address", "IPv6", "toIPv6('::')", true, true, null, null, "ip_address", "ipAddress"),
    HTTP_USER_AGENT("http_user_agent", "String", "''", false, false, "http", "user_agent", "http_user_agent", "httpUserAgent"),
    HTTP_ORIGIN("http_origin", "String", "''", false, false, "http", "origin", "http_origin", "httpOrigin"),
    HTTP_REFERER("http_referer", "String", "''", false, false, "http", "referer", "http_referer", "httpReferer"),
    HTTP_ACCEPT_LANGUAGE("http_accept_language", "String", "''", false, false, "http", "accept_language", "http_accept_language", "httpAcceptLanguage"),
    HTTP_VERSION("http_version", "LowCardinality(String)", "''", false, false, "http", "version", "http_version", "httpVersion"),
    HTTP_CONTENT_TYPE("http_content_type", "LowCardinality(String)", "''", false, false, "http", "content_type", "http_content_type", "httpContentType"),
    HTTP_REQUEST_BYTES("http_request_bytes", "UInt32", "0", false, false, "http", "request_bytes", "http_request_bytes", "httpRequestBytes"),
    HTTP_SEC_CH_UA("http_sec_ch_ua", "String", "''", false, false, "http", "sec_ch_ua", "http_sec_ch_ua", "httpSecChUa"),
    HTTP_SEC_CH_UA_MOBILE("http_sec_ch_ua_mobile", "LowCardinality(String)", "''", false, false, "http", "sec_ch_ua_mobile", "http_sec_ch_ua_mobile", "httpSecChUaMobile"),
    HTTP_SEC_CH_UA_PLATFORM("http_sec_ch_ua_platform", "LowCardinality(String)", "''", false, false, "http", "sec_ch_ua_platform", "http_sec_ch_ua_platform", "httpSecChUaPlatform"),
    GEO_COUNTRY_CODE("geo_country_code", "LowCardinality(String)", "''", true, true, null, null, "geo_country_code", "geoCountryCode"),
    GEO_REGION("geo_region", "LowCardinality(String)", "''", true, true, null, null, "geo_region", "geoRegion"),
    GEO_CITY("geo_city", "LowCardinality(String)", "''", true, true, null, null, "geo_city", "geoCity"),
    NETWORK_ASN("network_asn", "UInt32", "0", true, true, null, null, "network_asn", "networkAsn"),
    NETWORK_IS_DATACENTER("network_is_datacenter", "Int8", "-1", true, true, null, null, "network_is_datacenter", "networkIsDatacenter"),
    NETWORK_PROXY_TYPE("network_proxy_type", "LowCardinality(String)", "''", true, true, null, null, "network_proxy_type", "networkProxyType"),
    CUSTOM_PAYLOAD_RAW("custom_payload_raw", "String", "''", false, false, null, null, "custom_payload_raw", "customPayloadRaw"),
    CUSTOM_PAYLOAD_BYTES("custom_payload_bytes", "UInt32", "0", false, false, null, null, "custom_payload_bytes", "customPayloadBytes");

    private static final List<String> NAMES = Arrays.stream(values()).map(ClickstreamColumn::columnName).toList();

    private final String columnName;
    private final String clickHouseType;
    private final String defaultExpression;
    private final boolean serverOwned;
    private final boolean skipBrowserBind;
    private final String jsonGroup;
    private final String groupMember;
    private final List<String> aliases;

    ClickstreamColumn(
            String columnName,
            String clickHouseType,
            String defaultExpression,
            boolean serverOwned,
            boolean skipBrowserBind,
            String jsonGroup,
            String groupMember,
            String... aliases
    ) {
        this.columnName = columnName;
        this.clickHouseType = clickHouseType;
        this.defaultExpression = defaultExpression;
        this.serverOwned = serverOwned;
        this.skipBrowserBind = skipBrowserBind;
        this.jsonGroup = jsonGroup;
        this.groupMember = groupMember;
        this.aliases = List.of(aliases);
    }

    public static List<String> names() {
        return NAMES;
    }

    public String columnName() { return columnName; }
    public String clickHouseType() { return clickHouseType; }
    public boolean serverOwned() { return serverOwned; }
    public boolean skipBrowserBind() { return skipBrowserBind; }
    public String jsonGroup() { return jsonGroup; }
    public String groupMember() { return groupMember; }
    public List<String> aliases() { return aliases; }

    public String wireType() {
        if (clickHouseType.startsWith("LowCardinality(") && clickHouseType.endsWith(")")) {
            return clickHouseType.substring("LowCardinality(".length(), clickHouseType.length() - 1);
        }
        return clickHouseType;
    }

    public boolean floating() {
        return "Float32".equals(wireType()) || "Float64".equals(wireType());
    }

    public boolean internetAddress() {
        return "IPv6".equals(wireType()) || "IPv4".equals(wireType());
    }

    public boolean textual() {
        return "String".equals(wireType());
    }

    public int maxChars() {
        if (!textual()) {
            return 0;
        }
        if (this == CUSTOM_PAYLOAD_RAW) {
            return 32768;
        }
        return 2048;
    }

    public long minNumber() {
        return switch (wireType()) {
            case "Int8" -> Byte.MIN_VALUE;
            case "UInt8" -> 0L;
            case "Int16" -> Short.MIN_VALUE;
            case "UInt16" -> 0L;
            case "Int32" -> Integer.MIN_VALUE;
            case "UInt32" -> 0L;
            case "Int64" -> Long.MIN_VALUE;
            case "UInt64" -> 0L;
            default -> Long.MIN_VALUE;
        };
    }

    public long maxNumber() {
        return switch (wireType()) {
            case "Int8" -> Byte.MAX_VALUE;
            case "UInt8" -> 255L;
            case "Int16" -> Short.MAX_VALUE;
            case "UInt16" -> 65535L;
            case "Int32" -> Integer.MAX_VALUE;
            case "UInt32" -> 4294967295L;
            case "Int64", "UInt64" -> Long.MAX_VALUE;
            default -> Long.MAX_VALUE;
        };
    }

    public Object defaultValue() {
        if (internetAddress()) {
            return "::";
        }
        if (textual()) {
            if (defaultExpression == null || "''".equals(defaultExpression)) {
                return "";
            }
            throw new IllegalStateException("unsupported string default for " + columnName);
        }
        if (floating()) {
            return defaultExpression == null ? -1.0f : Float.parseFloat(defaultExpression);
        }
        if (defaultExpression == null) {
            return 0L;
        }
        return Long.parseLong(defaultExpression);
    }
}
