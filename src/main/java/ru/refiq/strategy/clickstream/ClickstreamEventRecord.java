package ru.refiq.strategy.clickstream;

/** Flat persistence row. One instance is one ClickHouse row. */
public final class ClickstreamEventRecord {

    private final Object[] values = new Object[ClickstreamColumn.values().length];

    public ClickstreamEventRecord() {
        for (ClickstreamColumn column : ClickstreamColumn.values()) {
            values[column.ordinal()] = column.defaultValue();
        }
    }

    public Object get(ClickstreamColumn column) {
        return values[column.ordinal()];
    }

    public void set(ClickstreamColumn column, Object value) {
        values[column.ordinal()] = value;
    }

    public long eventId() { return (long) values[ClickstreamColumn.EVENT_ID.ordinal()]; }
    public void setEventId(long value) { values[ClickstreamColumn.EVENT_ID.ordinal()] = value; }

    public long clientEventId() { return (long) values[ClickstreamColumn.CLIENT_EVENT_ID.ordinal()]; }
    public void setClientEventId(long value) { values[ClickstreamColumn.CLIENT_EVENT_ID.ordinal()] = value; }

    public long batchId() { return (long) values[ClickstreamColumn.BATCH_ID.ordinal()]; }
    public void setBatchId(long value) { values[ClickstreamColumn.BATCH_ID.ordinal()] = value; }

    public long schemaVersion() { return (long) values[ClickstreamColumn.SCHEMA_VERSION.ordinal()]; }
    public void setSchemaVersion(long value) { values[ClickstreamColumn.SCHEMA_VERSION.ordinal()] = value; }

    public String eventType() { return (String) values[ClickstreamColumn.EVENT_TYPE.ordinal()]; }
    public void setEventType(String value) { values[ClickstreamColumn.EVENT_TYPE.ordinal()] = value; }

    public String customEventName() { return (String) values[ClickstreamColumn.CUSTOM_EVENT_NAME.ordinal()]; }
    public void setCustomEventName(String value) { values[ClickstreamColumn.CUSTOM_EVENT_NAME.ordinal()] = value; }

    public long sequence() { return (long) values[ClickstreamColumn.SEQUENCE.ordinal()]; }
    public void setSequence(long value) { values[ClickstreamColumn.SEQUENCE.ordinal()] = value; }

    public long occurredTs() { return (long) values[ClickstreamColumn.OCCURRED_TS.ordinal()]; }
    public void setOccurredTs(long value) { values[ClickstreamColumn.OCCURRED_TS.ordinal()] = value; }

    public long sentTs() { return (long) values[ClickstreamColumn.SENT_TS.ordinal()]; }
    public void setSentTs(long value) { values[ClickstreamColumn.SENT_TS.ordinal()] = value; }

    public long capturedTs() { return (long) values[ClickstreamColumn.CAPTURED_TS.ordinal()]; }
    public void setCapturedTs(long value) { values[ClickstreamColumn.CAPTURED_TS.ordinal()] = value; }

    public long visitorId() { return (long) values[ClickstreamColumn.VISITOR_ID.ordinal()]; }
    public void setVisitorId(long value) { values[ClickstreamColumn.VISITOR_ID.ordinal()] = value; }

    public long sessionId() { return (long) values[ClickstreamColumn.SESSION_ID.ordinal()]; }
    public void setSessionId(long value) { values[ClickstreamColumn.SESSION_ID.ordinal()] = value; }

    public long pageViewId() { return (long) values[ClickstreamColumn.PAGE_VIEW_ID.ordinal()]; }
    public void setPageViewId(long value) { values[ClickstreamColumn.PAGE_VIEW_ID.ordinal()] = value; }

    public long previousPageViewId() { return (long) values[ClickstreamColumn.PREVIOUS_PAGE_VIEW_ID.ordinal()]; }
    public void setPreviousPageViewId(long value) { values[ClickstreamColumn.PREVIOUS_PAGE_VIEW_ID.ordinal()] = value; }

    public String siteKey() { return (String) values[ClickstreamColumn.SITE_KEY.ordinal()]; }
    public void setSiteKey(String value) { values[ClickstreamColumn.SITE_KEY.ordinal()] = value; }

    public long siteId() { return (long) values[ClickstreamColumn.SITE_ID.ordinal()]; }
    public void setSiteId(long value) { values[ClickstreamColumn.SITE_ID.ordinal()] = value; }

    public long businessId() { return (long) values[ClickstreamColumn.BUSINESS_ID.ordinal()]; }
    public void setBusinessId(long value) { values[ClickstreamColumn.BUSINESS_ID.ordinal()] = value; }

    public long partnerId() { return (long) values[ClickstreamColumn.PARTNER_ID.ordinal()]; }
    public void setPartnerId(long value) { values[ClickstreamColumn.PARTNER_ID.ordinal()] = value; }

    public long offerId() { return (long) values[ClickstreamColumn.OFFER_ID.ordinal()]; }
    public void setOfferId(long value) { values[ClickstreamColumn.OFFER_ID.ordinal()] = value; }

    public long campaignId() { return (long) values[ClickstreamColumn.CAMPAIGN_ID.ordinal()]; }
    public void setCampaignId(long value) { values[ClickstreamColumn.CAMPAIGN_ID.ordinal()] = value; }

    public long trackingLinkId() { return (long) values[ClickstreamColumn.TRACKING_LINK_ID.ordinal()]; }
    public void setTrackingLinkId(long value) { values[ClickstreamColumn.TRACKING_LINK_ID.ordinal()] = value; }

    public String trafficOwnership() { return (String) values[ClickstreamColumn.TRAFFIC_OWNERSHIP.ordinal()]; }
    public void setTrafficOwnership(String value) { values[ClickstreamColumn.TRAFFIC_OWNERSHIP.ordinal()] = value; }

    public String rqcid() { return (String) values[ClickstreamColumn.RQCID.ordinal()]; }
    public void setRqcid(String value) { values[ClickstreamColumn.RQCID.ordinal()] = value; }

    public long rqcidCapturedTs() { return (long) values[ClickstreamColumn.RQCID_CAPTURED_TS.ordinal()]; }
    public void setRqcidCapturedTs(long value) { values[ClickstreamColumn.RQCID_CAPTURED_TS.ordinal()] = value; }

    public String utmSource() { return (String) values[ClickstreamColumn.UTM_SOURCE.ordinal()]; }
    public void setUtmSource(String value) { values[ClickstreamColumn.UTM_SOURCE.ordinal()] = value; }

    public String utmMedium() { return (String) values[ClickstreamColumn.UTM_MEDIUM.ordinal()]; }
    public void setUtmMedium(String value) { values[ClickstreamColumn.UTM_MEDIUM.ordinal()] = value; }

    public String utmCampaign() { return (String) values[ClickstreamColumn.UTM_CAMPAIGN.ordinal()]; }
    public void setUtmCampaign(String value) { values[ClickstreamColumn.UTM_CAMPAIGN.ordinal()] = value; }

    public String utmContent() { return (String) values[ClickstreamColumn.UTM_CONTENT.ordinal()]; }
    public void setUtmContent(String value) { values[ClickstreamColumn.UTM_CONTENT.ordinal()] = value; }

    public String utmTerm() { return (String) values[ClickstreamColumn.UTM_TERM.ordinal()]; }
    public void setUtmTerm(String value) { values[ClickstreamColumn.UTM_TERM.ordinal()] = value; }

    public String yclid() { return (String) values[ClickstreamColumn.YCLID.ordinal()]; }
    public void setYclid(String value) { values[ClickstreamColumn.YCLID.ordinal()] = value; }

    public String gclid() { return (String) values[ClickstreamColumn.GCLID.ordinal()]; }
    public void setGclid(String value) { values[ClickstreamColumn.GCLID.ordinal()] = value; }

    public String fbclid() { return (String) values[ClickstreamColumn.FBCLID.ordinal()]; }
    public void setFbclid(String value) { values[ClickstreamColumn.FBCLID.ordinal()] = value; }

    public String ttclid() { return (String) values[ClickstreamColumn.TTCLID.ordinal()]; }
    public void setTtclid(String value) { values[ClickstreamColumn.TTCLID.ordinal()] = value; }

    public String vkClickId() { return (String) values[ClickstreamColumn.VK_CLICK_ID.ordinal()]; }
    public void setVkClickId(String value) { values[ClickstreamColumn.VK_CLICK_ID.ordinal()] = value; }

    public String landingOrigin() { return (String) values[ClickstreamColumn.LANDING_ORIGIN.ordinal()]; }
    public void setLandingOrigin(String value) { values[ClickstreamColumn.LANDING_ORIGIN.ordinal()] = value; }

    public String landingHostname() { return (String) values[ClickstreamColumn.LANDING_HOSTNAME.ordinal()]; }
    public void setLandingHostname(String value) { values[ClickstreamColumn.LANDING_HOSTNAME.ordinal()] = value; }

    public String landingPathname() { return (String) values[ClickstreamColumn.LANDING_PATHNAME.ordinal()]; }
    public void setLandingPathname(String value) { values[ClickstreamColumn.LANDING_PATHNAME.ordinal()] = value; }

    public String landingReferrerOrigin() { return (String) values[ClickstreamColumn.LANDING_REFERRER_ORIGIN.ordinal()]; }
    public void setLandingReferrerOrigin(String value) { values[ClickstreamColumn.LANDING_REFERRER_ORIGIN.ordinal()] = value; }

    public String landingReferrerHostname() { return (String) values[ClickstreamColumn.LANDING_REFERRER_HOSTNAME.ordinal()]; }
    public void setLandingReferrerHostname(String value) { values[ClickstreamColumn.LANDING_REFERRER_HOSTNAME.ordinal()] = value; }

    public String landingReferrerPathname() { return (String) values[ClickstreamColumn.LANDING_REFERRER_PATHNAME.ordinal()]; }
    public void setLandingReferrerPathname(String value) { values[ClickstreamColumn.LANDING_REFERRER_PATHNAME.ordinal()] = value; }

    public String pageProtocol() { return (String) values[ClickstreamColumn.PAGE_PROTOCOL.ordinal()]; }
    public void setPageProtocol(String value) { values[ClickstreamColumn.PAGE_PROTOCOL.ordinal()] = value; }

    public String pageOrigin() { return (String) values[ClickstreamColumn.PAGE_ORIGIN.ordinal()]; }
    public void setPageOrigin(String value) { values[ClickstreamColumn.PAGE_ORIGIN.ordinal()] = value; }

    public String pageHostname() { return (String) values[ClickstreamColumn.PAGE_HOSTNAME.ordinal()]; }
    public void setPageHostname(String value) { values[ClickstreamColumn.PAGE_HOSTNAME.ordinal()] = value; }

    public long pagePort() { return (long) values[ClickstreamColumn.PAGE_PORT.ordinal()]; }
    public void setPagePort(long value) { values[ClickstreamColumn.PAGE_PORT.ordinal()] = value; }

    public String pagePathname() { return (String) values[ClickstreamColumn.PAGE_PATHNAME.ordinal()]; }
    public void setPagePathname(String value) { values[ClickstreamColumn.PAGE_PATHNAME.ordinal()] = value; }

    public String pageHashRoute() { return (String) values[ClickstreamColumn.PAGE_HASH_ROUTE.ordinal()]; }
    public void setPageHashRoute(String value) { values[ClickstreamColumn.PAGE_HASH_ROUTE.ordinal()] = value; }

    public String pageTitle() { return (String) values[ClickstreamColumn.PAGE_TITLE.ordinal()]; }
    public void setPageTitle(String value) { values[ClickstreamColumn.PAGE_TITLE.ordinal()] = value; }

    public String pageContentType() { return (String) values[ClickstreamColumn.PAGE_CONTENT_TYPE.ordinal()]; }
    public void setPageContentType(String value) { values[ClickstreamColumn.PAGE_CONTENT_TYPE.ordinal()] = value; }

    public String pageCharset() { return (String) values[ClickstreamColumn.PAGE_CHARSET.ordinal()]; }
    public void setPageCharset(String value) { values[ClickstreamColumn.PAGE_CHARSET.ordinal()] = value; }

    public String pageNavigationType() { return (String) values[ClickstreamColumn.PAGE_NAVIGATION_TYPE.ordinal()]; }
    public void setPageNavigationType(String value) { values[ClickstreamColumn.PAGE_NAVIGATION_TYPE.ordinal()] = value; }

    public long pageHistoryLength() { return (long) values[ClickstreamColumn.PAGE_HISTORY_LENGTH.ordinal()]; }
    public void setPageHistoryLength(long value) { values[ClickstreamColumn.PAGE_HISTORY_LENGTH.ordinal()] = value; }

    public long pageIsTopFrame() { return (long) values[ClickstreamColumn.PAGE_IS_TOP_FRAME.ordinal()]; }
    public void setPageIsTopFrame(long value) { values[ClickstreamColumn.PAGE_IS_TOP_FRAME.ordinal()] = value; }

    public String pageReadyState() { return (String) values[ClickstreamColumn.PAGE_READY_STATE.ordinal()]; }
    public void setPageReadyState(String value) { values[ClickstreamColumn.PAGE_READY_STATE.ordinal()] = value; }

    public String pageVisibilityState() { return (String) values[ClickstreamColumn.PAGE_VISIBILITY_STATE.ordinal()]; }
    public void setPageVisibilityState(String value) { values[ClickstreamColumn.PAGE_VISIBILITY_STATE.ordinal()] = value; }

    public long pageDocumentWidth() { return (long) values[ClickstreamColumn.PAGE_DOCUMENT_WIDTH.ordinal()]; }
    public void setPageDocumentWidth(long value) { values[ClickstreamColumn.PAGE_DOCUMENT_WIDTH.ordinal()] = value; }

    public long pageDocumentHeight() { return (long) values[ClickstreamColumn.PAGE_DOCUMENT_HEIGHT.ordinal()]; }
    public void setPageDocumentHeight(long value) { values[ClickstreamColumn.PAGE_DOCUMENT_HEIGHT.ordinal()] = value; }

    public String pageReferrerOrigin() { return (String) values[ClickstreamColumn.PAGE_REFERRER_ORIGIN.ordinal()]; }
    public void setPageReferrerOrigin(String value) { values[ClickstreamColumn.PAGE_REFERRER_ORIGIN.ordinal()] = value; }

    public String pageReferrerHostname() { return (String) values[ClickstreamColumn.PAGE_REFERRER_HOSTNAME.ordinal()]; }
    public void setPageReferrerHostname(String value) { values[ClickstreamColumn.PAGE_REFERRER_HOSTNAME.ordinal()] = value; }

    public String pageReferrerPathname() { return (String) values[ClickstreamColumn.PAGE_REFERRER_PATHNAME.ordinal()]; }
    public void setPageReferrerPathname(String value) { values[ClickstreamColumn.PAGE_REFERRER_PATHNAME.ordinal()] = value; }

    public String navigatorUserAgent() { return (String) values[ClickstreamColumn.NAVIGATOR_USER_AGENT.ordinal()]; }
    public void setNavigatorUserAgent(String value) { values[ClickstreamColumn.NAVIGATOR_USER_AGENT.ordinal()] = value; }

    public String navigatorLanguage() { return (String) values[ClickstreamColumn.NAVIGATOR_LANGUAGE.ordinal()]; }
    public void setNavigatorLanguage(String value) { values[ClickstreamColumn.NAVIGATOR_LANGUAGE.ordinal()] = value; }

    public String navigatorLanguages() { return (String) values[ClickstreamColumn.NAVIGATOR_LANGUAGES.ordinal()]; }
    public void setNavigatorLanguages(String value) { values[ClickstreamColumn.NAVIGATOR_LANGUAGES.ordinal()] = value; }

    public String navigatorPlatform() { return (String) values[ClickstreamColumn.NAVIGATOR_PLATFORM.ordinal()]; }
    public void setNavigatorPlatform(String value) { values[ClickstreamColumn.NAVIGATOR_PLATFORM.ordinal()] = value; }

    public String navigatorVendor() { return (String) values[ClickstreamColumn.NAVIGATOR_VENDOR.ordinal()]; }
    public void setNavigatorVendor(String value) { values[ClickstreamColumn.NAVIGATOR_VENDOR.ordinal()] = value; }

    public long navigatorCookieEnabled() { return (long) values[ClickstreamColumn.NAVIGATOR_COOKIE_ENABLED.ordinal()]; }
    public void setNavigatorCookieEnabled(long value) { values[ClickstreamColumn.NAVIGATOR_COOKIE_ENABLED.ordinal()] = value; }

    public long navigatorWebdriver() { return (long) values[ClickstreamColumn.NAVIGATOR_WEBDRIVER.ordinal()]; }
    public void setNavigatorWebdriver(long value) { values[ClickstreamColumn.NAVIGATOR_WEBDRIVER.ordinal()] = value; }

    public long navigatorPdfViewerEnabled() { return (long) values[ClickstreamColumn.NAVIGATOR_PDF_VIEWER_ENABLED.ordinal()]; }
    public void setNavigatorPdfViewerEnabled(long value) { values[ClickstreamColumn.NAVIGATOR_PDF_VIEWER_ENABLED.ordinal()] = value; }

    public long navigatorHardwareConcurrency() { return (long) values[ClickstreamColumn.NAVIGATOR_HARDWARE_CONCURRENCY.ordinal()]; }
    public void setNavigatorHardwareConcurrency(long value) { values[ClickstreamColumn.NAVIGATOR_HARDWARE_CONCURRENCY.ordinal()] = value; }

    public float navigatorDeviceMemoryGb() { return (float) values[ClickstreamColumn.NAVIGATOR_DEVICE_MEMORY_GB.ordinal()]; }
    public void setNavigatorDeviceMemoryGb(float value) { values[ClickstreamColumn.NAVIGATOR_DEVICE_MEMORY_GB.ordinal()] = value; }

    public long navigatorMaxTouchPoints() { return (long) values[ClickstreamColumn.NAVIGATOR_MAX_TOUCH_POINTS.ordinal()]; }
    public void setNavigatorMaxTouchPoints(long value) { values[ClickstreamColumn.NAVIGATOR_MAX_TOUCH_POINTS.ordinal()] = value; }

    public long navigatorUaMobile() { return (long) values[ClickstreamColumn.NAVIGATOR_UA_MOBILE.ordinal()]; }
    public void setNavigatorUaMobile(long value) { values[ClickstreamColumn.NAVIGATOR_UA_MOBILE.ordinal()] = value; }

    public String navigatorUaPlatform() { return (String) values[ClickstreamColumn.NAVIGATOR_UA_PLATFORM.ordinal()]; }
    public void setNavigatorUaPlatform(String value) { values[ClickstreamColumn.NAVIGATOR_UA_PLATFORM.ordinal()] = value; }

    public String navigatorUaBrands() { return (String) values[ClickstreamColumn.NAVIGATOR_UA_BRANDS.ordinal()]; }
    public void setNavigatorUaBrands(String value) { values[ClickstreamColumn.NAVIGATOR_UA_BRANDS.ordinal()] = value; }

    public long screenWidth() { return (long) values[ClickstreamColumn.SCREEN_WIDTH.ordinal()]; }
    public void setScreenWidth(long value) { values[ClickstreamColumn.SCREEN_WIDTH.ordinal()] = value; }

    public long screenHeight() { return (long) values[ClickstreamColumn.SCREEN_HEIGHT.ordinal()]; }
    public void setScreenHeight(long value) { values[ClickstreamColumn.SCREEN_HEIGHT.ordinal()] = value; }

    public long screenAvailWidth() { return (long) values[ClickstreamColumn.SCREEN_AVAIL_WIDTH.ordinal()]; }
    public void setScreenAvailWidth(long value) { values[ClickstreamColumn.SCREEN_AVAIL_WIDTH.ordinal()] = value; }

    public long screenAvailHeight() { return (long) values[ClickstreamColumn.SCREEN_AVAIL_HEIGHT.ordinal()]; }
    public void setScreenAvailHeight(long value) { values[ClickstreamColumn.SCREEN_AVAIL_HEIGHT.ordinal()] = value; }

    public long screenColorDepth() { return (long) values[ClickstreamColumn.SCREEN_COLOR_DEPTH.ordinal()]; }
    public void setScreenColorDepth(long value) { values[ClickstreamColumn.SCREEN_COLOR_DEPTH.ordinal()] = value; }

    public long screenPixelDepth() { return (long) values[ClickstreamColumn.SCREEN_PIXEL_DEPTH.ordinal()]; }
    public void setScreenPixelDepth(long value) { values[ClickstreamColumn.SCREEN_PIXEL_DEPTH.ordinal()] = value; }

    public String screenOrientationType() { return (String) values[ClickstreamColumn.SCREEN_ORIENTATION_TYPE.ordinal()]; }
    public void setScreenOrientationType(String value) { values[ClickstreamColumn.SCREEN_ORIENTATION_TYPE.ordinal()] = value; }

    public long screenOrientationAngle() { return (long) values[ClickstreamColumn.SCREEN_ORIENTATION_ANGLE.ordinal()]; }
    public void setScreenOrientationAngle(long value) { values[ClickstreamColumn.SCREEN_ORIENTATION_ANGLE.ordinal()] = value; }

    public float screenDevicePixelRatio() { return (float) values[ClickstreamColumn.SCREEN_DEVICE_PIXEL_RATIO.ordinal()]; }
    public void setScreenDevicePixelRatio(float value) { values[ClickstreamColumn.SCREEN_DEVICE_PIXEL_RATIO.ordinal()] = value; }

    public long viewportWidth() { return (long) values[ClickstreamColumn.VIEWPORT_WIDTH.ordinal()]; }
    public void setViewportWidth(long value) { values[ClickstreamColumn.VIEWPORT_WIDTH.ordinal()] = value; }

    public long viewportHeight() { return (long) values[ClickstreamColumn.VIEWPORT_HEIGHT.ordinal()]; }
    public void setViewportHeight(long value) { values[ClickstreamColumn.VIEWPORT_HEIGHT.ordinal()] = value; }

    public long windowOuterWidth() { return (long) values[ClickstreamColumn.WINDOW_OUTER_WIDTH.ordinal()]; }
    public void setWindowOuterWidth(long value) { values[ClickstreamColumn.WINDOW_OUTER_WIDTH.ordinal()] = value; }

    public long windowOuterHeight() { return (long) values[ClickstreamColumn.WINDOW_OUTER_HEIGHT.ordinal()]; }
    public void setWindowOuterHeight(long value) { values[ClickstreamColumn.WINDOW_OUTER_HEIGHT.ordinal()] = value; }

    public float visualViewportWidth() { return (float) values[ClickstreamColumn.VISUAL_VIEWPORT_WIDTH.ordinal()]; }
    public void setVisualViewportWidth(float value) { values[ClickstreamColumn.VISUAL_VIEWPORT_WIDTH.ordinal()] = value; }

    public float visualViewportHeight() { return (float) values[ClickstreamColumn.VISUAL_VIEWPORT_HEIGHT.ordinal()]; }
    public void setVisualViewportHeight(float value) { values[ClickstreamColumn.VISUAL_VIEWPORT_HEIGHT.ordinal()] = value; }

    public float visualViewportScale() { return (float) values[ClickstreamColumn.VISUAL_VIEWPORT_SCALE.ordinal()]; }
    public void setVisualViewportScale(float value) { values[ClickstreamColumn.VISUAL_VIEWPORT_SCALE.ordinal()] = value; }

    public float visualViewportOffsetLeft() { return (float) values[ClickstreamColumn.VISUAL_VIEWPORT_OFFSET_LEFT.ordinal()]; }
    public void setVisualViewportOffsetLeft(float value) { values[ClickstreamColumn.VISUAL_VIEWPORT_OFFSET_LEFT.ordinal()] = value; }

    public float visualViewportOffsetTop() { return (float) values[ClickstreamColumn.VISUAL_VIEWPORT_OFFSET_TOP.ordinal()]; }
    public void setVisualViewportOffsetTop(float value) { values[ClickstreamColumn.VISUAL_VIEWPORT_OFFSET_TOP.ordinal()] = value; }

    public String mediaColorScheme() { return (String) values[ClickstreamColumn.MEDIA_COLOR_SCHEME.ordinal()]; }
    public void setMediaColorScheme(String value) { values[ClickstreamColumn.MEDIA_COLOR_SCHEME.ordinal()] = value; }

    public long mediaReducedMotion() { return (long) values[ClickstreamColumn.MEDIA_REDUCED_MOTION.ordinal()]; }
    public void setMediaReducedMotion(long value) { values[ClickstreamColumn.MEDIA_REDUCED_MOTION.ordinal()] = value; }

    public String mediaPreferredContrast() { return (String) values[ClickstreamColumn.MEDIA_PREFERRED_CONTRAST.ordinal()]; }
    public void setMediaPreferredContrast(String value) { values[ClickstreamColumn.MEDIA_PREFERRED_CONTRAST.ordinal()] = value; }

    public String mediaPointer() { return (String) values[ClickstreamColumn.MEDIA_POINTER.ordinal()]; }
    public void setMediaPointer(String value) { values[ClickstreamColumn.MEDIA_POINTER.ordinal()] = value; }

    public String mediaAnyPointer() { return (String) values[ClickstreamColumn.MEDIA_ANY_POINTER.ordinal()]; }
    public void setMediaAnyPointer(String value) { values[ClickstreamColumn.MEDIA_ANY_POINTER.ordinal()] = value; }

    public long mediaHover() { return (long) values[ClickstreamColumn.MEDIA_HOVER.ordinal()]; }
    public void setMediaHover(long value) { values[ClickstreamColumn.MEDIA_HOVER.ordinal()] = value; }

    public long mediaAnyHover() { return (long) values[ClickstreamColumn.MEDIA_ANY_HOVER.ordinal()]; }
    public void setMediaAnyHover(long value) { values[ClickstreamColumn.MEDIA_ANY_HOVER.ordinal()] = value; }

    public String mediaDisplayMode() { return (String) values[ClickstreamColumn.MEDIA_DISPLAY_MODE.ordinal()]; }
    public void setMediaDisplayMode(String value) { values[ClickstreamColumn.MEDIA_DISPLAY_MODE.ordinal()] = value; }

    public String locale() { return (String) values[ClickstreamColumn.LOCALE.ordinal()]; }
    public void setLocale(String value) { values[ClickstreamColumn.LOCALE.ordinal()] = value; }

    public String timezone() { return (String) values[ClickstreamColumn.TIMEZONE.ordinal()]; }
    public void setTimezone(String value) { values[ClickstreamColumn.TIMEZONE.ordinal()] = value; }

    public long timezoneOffsetMinutes() { return (long) values[ClickstreamColumn.TIMEZONE_OFFSET_MINUTES.ordinal()]; }
    public void setTimezoneOffsetMinutes(long value) { values[ClickstreamColumn.TIMEZONE_OFFSET_MINUTES.ordinal()] = value; }

    public String calendar() { return (String) values[ClickstreamColumn.CALENDAR.ordinal()]; }
    public void setCalendar(String value) { values[ClickstreamColumn.CALENDAR.ordinal()] = value; }

    public String numberingSystem() { return (String) values[ClickstreamColumn.NUMBERING_SYSTEM.ordinal()]; }
    public void setNumberingSystem(String value) { values[ClickstreamColumn.NUMBERING_SYSTEM.ordinal()] = value; }

    public long networkOnline() { return (long) values[ClickstreamColumn.NETWORK_ONLINE.ordinal()]; }
    public void setNetworkOnline(long value) { values[ClickstreamColumn.NETWORK_ONLINE.ordinal()] = value; }

    public String networkType() { return (String) values[ClickstreamColumn.NETWORK_TYPE.ordinal()]; }
    public void setNetworkType(String value) { values[ClickstreamColumn.NETWORK_TYPE.ordinal()] = value; }

    public String networkEffectiveType() { return (String) values[ClickstreamColumn.NETWORK_EFFECTIVE_TYPE.ordinal()]; }
    public void setNetworkEffectiveType(String value) { values[ClickstreamColumn.NETWORK_EFFECTIVE_TYPE.ordinal()] = value; }

    public float networkDownlinkMbps() { return (float) values[ClickstreamColumn.NETWORK_DOWNLINK_MBPS.ordinal()]; }
    public void setNetworkDownlinkMbps(float value) { values[ClickstreamColumn.NETWORK_DOWNLINK_MBPS.ordinal()] = value; }

    public long networkRtt() { return (long) values[ClickstreamColumn.NETWORK_RTT.ordinal()]; }
    public void setNetworkRtt(long value) { values[ClickstreamColumn.NETWORK_RTT.ordinal()] = value; }

    public long networkSaveData() { return (long) values[ClickstreamColumn.NETWORK_SAVE_DATA.ordinal()]; }
    public void setNetworkSaveData(long value) { values[ClickstreamColumn.NETWORK_SAVE_DATA.ordinal()] = value; }

    public long performanceTimeOriginTs() { return (long) values[ClickstreamColumn.PERFORMANCE_TIME_ORIGIN_TS.ordinal()]; }
    public void setPerformanceTimeOriginTs(long value) { values[ClickstreamColumn.PERFORMANCE_TIME_ORIGIN_TS.ordinal()] = value; }

    public long performanceRedirectCount() { return (long) values[ClickstreamColumn.PERFORMANCE_REDIRECT_COUNT.ordinal()]; }
    public void setPerformanceRedirectCount(long value) { values[ClickstreamColumn.PERFORMANCE_REDIRECT_COUNT.ordinal()] = value; }

    public long performanceRedirect() { return (long) values[ClickstreamColumn.PERFORMANCE_REDIRECT.ordinal()]; }
    public void setPerformanceRedirect(long value) { values[ClickstreamColumn.PERFORMANCE_REDIRECT.ordinal()] = value; }

    public long performanceDns() { return (long) values[ClickstreamColumn.PERFORMANCE_DNS.ordinal()]; }
    public void setPerformanceDns(long value) { values[ClickstreamColumn.PERFORMANCE_DNS.ordinal()] = value; }

    public long performanceTcp() { return (long) values[ClickstreamColumn.PERFORMANCE_TCP.ordinal()]; }
    public void setPerformanceTcp(long value) { values[ClickstreamColumn.PERFORMANCE_TCP.ordinal()] = value; }

    public long performanceTls() { return (long) values[ClickstreamColumn.PERFORMANCE_TLS.ordinal()]; }
    public void setPerformanceTls(long value) { values[ClickstreamColumn.PERFORMANCE_TLS.ordinal()] = value; }

    public long performanceTtfb() { return (long) values[ClickstreamColumn.PERFORMANCE_TTFB.ordinal()]; }
    public void setPerformanceTtfb(long value) { values[ClickstreamColumn.PERFORMANCE_TTFB.ordinal()] = value; }

    public long performanceResponse() { return (long) values[ClickstreamColumn.PERFORMANCE_RESPONSE.ordinal()]; }
    public void setPerformanceResponse(long value) { values[ClickstreamColumn.PERFORMANCE_RESPONSE.ordinal()] = value; }

    public long performanceDomInteractive() { return (long) values[ClickstreamColumn.PERFORMANCE_DOM_INTERACTIVE.ordinal()]; }
    public void setPerformanceDomInteractive(long value) { values[ClickstreamColumn.PERFORMANCE_DOM_INTERACTIVE.ordinal()] = value; }

    public long performanceDomContentLoaded() { return (long) values[ClickstreamColumn.PERFORMANCE_DOM_CONTENT_LOADED.ordinal()]; }
    public void setPerformanceDomContentLoaded(long value) { values[ClickstreamColumn.PERFORMANCE_DOM_CONTENT_LOADED.ordinal()] = value; }

    public long performanceLoad() { return (long) values[ClickstreamColumn.PERFORMANCE_LOAD.ordinal()]; }
    public void setPerformanceLoad(long value) { values[ClickstreamColumn.PERFORMANCE_LOAD.ordinal()] = value; }

    public String performanceNextHopProtocol() { return (String) values[ClickstreamColumn.PERFORMANCE_NEXT_HOP_PROTOCOL.ordinal()]; }
    public void setPerformanceNextHopProtocol(String value) { values[ClickstreamColumn.PERFORMANCE_NEXT_HOP_PROTOCOL.ordinal()] = value; }

    public long performanceTransferSize() { return (long) values[ClickstreamColumn.PERFORMANCE_TRANSFER_SIZE.ordinal()]; }
    public void setPerformanceTransferSize(long value) { values[ClickstreamColumn.PERFORMANCE_TRANSFER_SIZE.ordinal()] = value; }

    public long performanceEncodedBodySize() { return (long) values[ClickstreamColumn.PERFORMANCE_ENCODED_BODY_SIZE.ordinal()]; }
    public void setPerformanceEncodedBodySize(long value) { values[ClickstreamColumn.PERFORMANCE_ENCODED_BODY_SIZE.ordinal()] = value; }

    public long performanceDecodedBodySize() { return (long) values[ClickstreamColumn.PERFORMANCE_DECODED_BODY_SIZE.ordinal()]; }
    public void setPerformanceDecodedBodySize(long value) { values[ClickstreamColumn.PERFORMANCE_DECODED_BODY_SIZE.ordinal()] = value; }

    public long webVitalFp() { return (long) values[ClickstreamColumn.WEB_VITAL_FP.ordinal()]; }
    public void setWebVitalFp(long value) { values[ClickstreamColumn.WEB_VITAL_FP.ordinal()] = value; }

    public long webVitalFcp() { return (long) values[ClickstreamColumn.WEB_VITAL_FCP.ordinal()]; }
    public void setWebVitalFcp(long value) { values[ClickstreamColumn.WEB_VITAL_FCP.ordinal()] = value; }

    public long webVitalLcp() { return (long) values[ClickstreamColumn.WEB_VITAL_LCP.ordinal()]; }
    public void setWebVitalLcp(long value) { values[ClickstreamColumn.WEB_VITAL_LCP.ordinal()] = value; }

    public float webVitalCls() { return (float) values[ClickstreamColumn.WEB_VITAL_CLS.ordinal()]; }
    public void setWebVitalCls(float value) { values[ClickstreamColumn.WEB_VITAL_CLS.ordinal()] = value; }

    public long webVitalInp() { return (long) values[ClickstreamColumn.WEB_VITAL_INP.ordinal()]; }
    public void setWebVitalInp(long value) { values[ClickstreamColumn.WEB_VITAL_INP.ordinal()] = value; }

    public long resourceCount() { return (long) values[ClickstreamColumn.RESOURCE_COUNT.ordinal()]; }
    public void setResourceCount(long value) { values[ClickstreamColumn.RESOURCE_COUNT.ordinal()] = value; }

    public long resourceScriptCount() { return (long) values[ClickstreamColumn.RESOURCE_SCRIPT_COUNT.ordinal()]; }
    public void setResourceScriptCount(long value) { values[ClickstreamColumn.RESOURCE_SCRIPT_COUNT.ordinal()] = value; }

    public long resourceImageCount() { return (long) values[ClickstreamColumn.RESOURCE_IMAGE_COUNT.ordinal()]; }
    public void setResourceImageCount(long value) { values[ClickstreamColumn.RESOURCE_IMAGE_COUNT.ordinal()] = value; }

    public long resourceXhrFetchCount() { return (long) values[ClickstreamColumn.RESOURCE_XHR_FETCH_COUNT.ordinal()]; }
    public void setResourceXhrFetchCount(long value) { values[ClickstreamColumn.RESOURCE_XHR_FETCH_COUNT.ordinal()] = value; }

    public long resourceStylesheetCount() { return (long) values[ClickstreamColumn.RESOURCE_STYLESHEET_COUNT.ordinal()]; }
    public void setResourceStylesheetCount(long value) { values[ClickstreamColumn.RESOURCE_STYLESHEET_COUNT.ordinal()] = value; }

    public long resourceFontCount() { return (long) values[ClickstreamColumn.RESOURCE_FONT_COUNT.ordinal()]; }
    public void setResourceFontCount(long value) { values[ClickstreamColumn.RESOURCE_FONT_COUNT.ordinal()] = value; }

    public long resourceMediaCount() { return (long) values[ClickstreamColumn.RESOURCE_MEDIA_COUNT.ordinal()]; }
    public void setResourceMediaCount(long value) { values[ClickstreamColumn.RESOURCE_MEDIA_COUNT.ordinal()] = value; }

    public long resourceFailedCount() { return (long) values[ClickstreamColumn.RESOURCE_FAILED_COUNT.ordinal()]; }
    public void setResourceFailedCount(long value) { values[ClickstreamColumn.RESOURCE_FAILED_COUNT.ordinal()] = value; }

    public long resourceTransferBytes() { return (long) values[ClickstreamColumn.RESOURCE_TRANSFER_BYTES.ordinal()]; }
    public void setResourceTransferBytes(long value) { values[ClickstreamColumn.RESOURCE_TRANSFER_BYTES.ordinal()] = value; }

    public long resourceSlowest() { return (long) values[ClickstreamColumn.RESOURCE_SLOWEST.ordinal()]; }
    public void setResourceSlowest(long value) { values[ClickstreamColumn.RESOURCE_SLOWEST.ordinal()] = value; }

    public long clickClientX() { return (long) values[ClickstreamColumn.CLICK_CLIENT_X.ordinal()]; }
    public void setClickClientX(long value) { values[ClickstreamColumn.CLICK_CLIENT_X.ordinal()] = value; }

    public long clickClientY() { return (long) values[ClickstreamColumn.CLICK_CLIENT_Y.ordinal()]; }
    public void setClickClientY(long value) { values[ClickstreamColumn.CLICK_CLIENT_Y.ordinal()] = value; }

    public long clickPageX() { return (long) values[ClickstreamColumn.CLICK_PAGE_X.ordinal()]; }
    public void setClickPageX(long value) { values[ClickstreamColumn.CLICK_PAGE_X.ordinal()] = value; }

    public long clickPageY() { return (long) values[ClickstreamColumn.CLICK_PAGE_Y.ordinal()]; }
    public void setClickPageY(long value) { values[ClickstreamColumn.CLICK_PAGE_Y.ordinal()] = value; }

    public float clickRelativeX() { return (float) values[ClickstreamColumn.CLICK_RELATIVE_X.ordinal()]; }
    public void setClickRelativeX(float value) { values[ClickstreamColumn.CLICK_RELATIVE_X.ordinal()] = value; }

    public float clickRelativeY() { return (float) values[ClickstreamColumn.CLICK_RELATIVE_Y.ordinal()]; }
    public void setClickRelativeY(float value) { values[ClickstreamColumn.CLICK_RELATIVE_Y.ordinal()] = value; }

    public long clickButton() { return (long) values[ClickstreamColumn.CLICK_BUTTON.ordinal()]; }
    public void setClickButton(long value) { values[ClickstreamColumn.CLICK_BUTTON.ordinal()] = value; }

    public String clickPointerType() { return (String) values[ClickstreamColumn.CLICK_POINTER_TYPE.ordinal()]; }
    public void setClickPointerType(String value) { values[ClickstreamColumn.CLICK_POINTER_TYPE.ordinal()] = value; }

    public long clickCtrlKey() { return (long) values[ClickstreamColumn.CLICK_CTRL_KEY.ordinal()]; }
    public void setClickCtrlKey(long value) { values[ClickstreamColumn.CLICK_CTRL_KEY.ordinal()] = value; }

    public long clickShiftKey() { return (long) values[ClickstreamColumn.CLICK_SHIFT_KEY.ordinal()]; }
    public void setClickShiftKey(long value) { values[ClickstreamColumn.CLICK_SHIFT_KEY.ordinal()] = value; }

    public long clickAltKey() { return (long) values[ClickstreamColumn.CLICK_ALT_KEY.ordinal()]; }
    public void setClickAltKey(long value) { values[ClickstreamColumn.CLICK_ALT_KEY.ordinal()] = value; }

    public long clickMetaKey() { return (long) values[ClickstreamColumn.CLICK_META_KEY.ordinal()]; }
    public void setClickMetaKey(long value) { values[ClickstreamColumn.CLICK_META_KEY.ordinal()] = value; }

    public float pointerPressure() { return (float) values[ClickstreamColumn.POINTER_PRESSURE.ordinal()]; }
    public void setPointerPressure(float value) { values[ClickstreamColumn.POINTER_PRESSURE.ordinal()] = value; }

    public float pointerWidth() { return (float) values[ClickstreamColumn.POINTER_WIDTH.ordinal()]; }
    public void setPointerWidth(float value) { values[ClickstreamColumn.POINTER_WIDTH.ordinal()] = value; }

    public float pointerHeight() { return (float) values[ClickstreamColumn.POINTER_HEIGHT.ordinal()]; }
    public void setPointerHeight(float value) { values[ClickstreamColumn.POINTER_HEIGHT.ordinal()] = value; }

    public long pointerIsPrimary() { return (long) values[ClickstreamColumn.POINTER_IS_PRIMARY.ordinal()]; }
    public void setPointerIsPrimary(long value) { values[ClickstreamColumn.POINTER_IS_PRIMARY.ordinal()] = value; }

    public String targetTrackingId() { return (String) values[ClickstreamColumn.TARGET_TRACKING_ID.ordinal()]; }
    public void setTargetTrackingId(String value) { values[ClickstreamColumn.TARGET_TRACKING_ID.ordinal()] = value; }

    public String targetElementId() { return (String) values[ClickstreamColumn.TARGET_ELEMENT_ID.ordinal()]; }
    public void setTargetElementId(String value) { values[ClickstreamColumn.TARGET_ELEMENT_ID.ordinal()] = value; }

    public String targetTag() { return (String) values[ClickstreamColumn.TARGET_TAG.ordinal()]; }
    public void setTargetTag(String value) { values[ClickstreamColumn.TARGET_TAG.ordinal()] = value; }

    public String targetRole() { return (String) values[ClickstreamColumn.TARGET_ROLE.ordinal()]; }
    public void setTargetRole(String value) { values[ClickstreamColumn.TARGET_ROLE.ordinal()] = value; }

    public String targetHrefOrigin() { return (String) values[ClickstreamColumn.TARGET_HREF_ORIGIN.ordinal()]; }
    public void setTargetHrefOrigin(String value) { values[ClickstreamColumn.TARGET_HREF_ORIGIN.ordinal()] = value; }

    public String targetHrefHostname() { return (String) values[ClickstreamColumn.TARGET_HREF_HOSTNAME.ordinal()]; }
    public void setTargetHrefHostname(String value) { values[ClickstreamColumn.TARGET_HREF_HOSTNAME.ordinal()] = value; }

    public String targetHrefPathname() { return (String) values[ClickstreamColumn.TARGET_HREF_PATHNAME.ordinal()]; }
    public void setTargetHrefPathname(String value) { values[ClickstreamColumn.TARGET_HREF_PATHNAME.ordinal()] = value; }

    public long targetIsExternal() { return (long) values[ClickstreamColumn.TARGET_IS_EXTERNAL.ordinal()]; }
    public void setTargetIsExternal(long value) { values[ClickstreamColumn.TARGET_IS_EXTERNAL.ordinal()] = value; }

    public float targetWidth() { return (float) values[ClickstreamColumn.TARGET_WIDTH.ordinal()]; }
    public void setTargetWidth(float value) { values[ClickstreamColumn.TARGET_WIDTH.ordinal()] = value; }

    public float targetHeight() { return (float) values[ClickstreamColumn.TARGET_HEIGHT.ordinal()]; }
    public void setTargetHeight(float value) { values[ClickstreamColumn.TARGET_HEIGHT.ordinal()] = value; }

    public long scrollX() { return (long) values[ClickstreamColumn.SCROLL_X.ordinal()]; }
    public void setScrollX(long value) { values[ClickstreamColumn.SCROLL_X.ordinal()] = value; }

    public long scrollY() { return (long) values[ClickstreamColumn.SCROLL_Y.ordinal()]; }
    public void setScrollY(long value) { values[ClickstreamColumn.SCROLL_Y.ordinal()] = value; }

    public long scrollDocumentHeight() { return (long) values[ClickstreamColumn.SCROLL_DOCUMENT_HEIGHT.ordinal()]; }
    public void setScrollDocumentHeight(long value) { values[ClickstreamColumn.SCROLL_DOCUMENT_HEIGHT.ordinal()] = value; }

    public long scrollViewportHeight() { return (long) values[ClickstreamColumn.SCROLL_VIEWPORT_HEIGHT.ordinal()]; }
    public void setScrollViewportHeight(long value) { values[ClickstreamColumn.SCROLL_VIEWPORT_HEIGHT.ordinal()] = value; }

    public float scrollDepth() { return (float) values[ClickstreamColumn.SCROLL_DEPTH.ordinal()]; }
    public void setScrollDepth(float value) { values[ClickstreamColumn.SCROLL_DEPTH.ordinal()] = value; }

    public float scrollMaxDepth() { return (float) values[ClickstreamColumn.SCROLL_MAX_DEPTH.ordinal()]; }
    public void setScrollMaxDepth(float value) { values[ClickstreamColumn.SCROLL_MAX_DEPTH.ordinal()] = value; }

    public String scrollDirection() { return (String) values[ClickstreamColumn.SCROLL_DIRECTION.ordinal()]; }
    public void setScrollDirection(String value) { values[ClickstreamColumn.SCROLL_DIRECTION.ordinal()] = value; }

    public long scrollMilestone() { return (long) values[ClickstreamColumn.SCROLL_MILESTONE.ordinal()]; }
    public void setScrollMilestone(long value) { values[ClickstreamColumn.SCROLL_MILESTONE.ordinal()] = value; }

    public long scrollTimeTo25() { return (long) values[ClickstreamColumn.SCROLL_TIME_TO_25.ordinal()]; }
    public void setScrollTimeTo25(long value) { values[ClickstreamColumn.SCROLL_TIME_TO_25.ordinal()] = value; }

    public long scrollTimeTo50() { return (long) values[ClickstreamColumn.SCROLL_TIME_TO_50.ordinal()]; }
    public void setScrollTimeTo50(long value) { values[ClickstreamColumn.SCROLL_TIME_TO_50.ordinal()] = value; }

    public long scrollTimeTo75() { return (long) values[ClickstreamColumn.SCROLL_TIME_TO_75.ordinal()]; }
    public void setScrollTimeTo75(long value) { values[ClickstreamColumn.SCROLL_TIME_TO_75.ordinal()] = value; }

    public long scrollTimeTo90() { return (long) values[ClickstreamColumn.SCROLL_TIME_TO_90.ordinal()]; }
    public void setScrollTimeTo90(long value) { values[ClickstreamColumn.SCROLL_TIME_TO_90.ordinal()] = value; }

    public long scrollTimeTo100() { return (long) values[ClickstreamColumn.SCROLL_TIME_TO_100.ordinal()]; }
    public void setScrollTimeTo100(long value) { values[ClickstreamColumn.SCROLL_TIME_TO_100.ordinal()] = value; }

    public long engagementTotalDuration() { return (long) values[ClickstreamColumn.ENGAGEMENT_TOTAL_DURATION.ordinal()]; }
    public void setEngagementTotalDuration(long value) { values[ClickstreamColumn.ENGAGEMENT_TOTAL_DURATION.ordinal()] = value; }

    public long engagementVisibleDuration() { return (long) values[ClickstreamColumn.ENGAGEMENT_VISIBLE_DURATION.ordinal()]; }
    public void setEngagementVisibleDuration(long value) { values[ClickstreamColumn.ENGAGEMENT_VISIBLE_DURATION.ordinal()] = value; }

    public long engagementActiveDuration() { return (long) values[ClickstreamColumn.ENGAGEMENT_ACTIVE_DURATION.ordinal()]; }
    public void setEngagementActiveDuration(long value) { values[ClickstreamColumn.ENGAGEMENT_ACTIVE_DURATION.ordinal()] = value; }

    public long engagementTimeToFirstInteraction() { return (long) values[ClickstreamColumn.ENGAGEMENT_TIME_TO_FIRST_INTERACTION.ordinal()]; }
    public void setEngagementTimeToFirstInteraction(long value) { values[ClickstreamColumn.ENGAGEMENT_TIME_TO_FIRST_INTERACTION.ordinal()] = value; }

    public long engagementClickCount() { return (long) values[ClickstreamColumn.ENGAGEMENT_CLICK_COUNT.ordinal()]; }
    public void setEngagementClickCount(long value) { values[ClickstreamColumn.ENGAGEMENT_CLICK_COUNT.ordinal()] = value; }

    public long engagementScrollCount() { return (long) values[ClickstreamColumn.ENGAGEMENT_SCROLL_COUNT.ordinal()]; }
    public void setEngagementScrollCount(long value) { values[ClickstreamColumn.ENGAGEMENT_SCROLL_COUNT.ordinal()] = value; }

    public float engagementMaxScrollDepth() { return (float) values[ClickstreamColumn.ENGAGEMENT_MAX_SCROLL_DEPTH.ordinal()]; }
    public void setEngagementMaxScrollDepth(float value) { values[ClickstreamColumn.ENGAGEMENT_MAX_SCROLL_DEPTH.ordinal()] = value; }

    public String engagementExitReason() { return (String) values[ClickstreamColumn.ENGAGEMENT_EXIT_REASON.ordinal()]; }
    public void setEngagementExitReason(String value) { values[ClickstreamColumn.ENGAGEMENT_EXIT_REASON.ordinal()] = value; }

    public String formTrackingId() { return (String) values[ClickstreamColumn.FORM_TRACKING_ID.ordinal()]; }
    public void setFormTrackingId(String value) { values[ClickstreamColumn.FORM_TRACKING_ID.ordinal()] = value; }

    public String formElementId() { return (String) values[ClickstreamColumn.FORM_ELEMENT_ID.ordinal()]; }
    public void setFormElementId(String value) { values[ClickstreamColumn.FORM_ELEMENT_ID.ordinal()] = value; }

    public long formFieldCount() { return (long) values[ClickstreamColumn.FORM_FIELD_COUNT.ordinal()]; }
    public void setFormFieldCount(long value) { values[ClickstreamColumn.FORM_FIELD_COUNT.ordinal()] = value; }

    public long formValid() { return (long) values[ClickstreamColumn.FORM_VALID.ordinal()]; }
    public void setFormValid(long value) { values[ClickstreamColumn.FORM_VALID.ordinal()] = value; }

    public String formMethod() { return (String) values[ClickstreamColumn.FORM_METHOD.ordinal()]; }
    public void setFormMethod(String value) { values[ClickstreamColumn.FORM_METHOD.ordinal()] = value; }

    public String mediaTrackingId() { return (String) values[ClickstreamColumn.MEDIA_TRACKING_ID.ordinal()]; }
    public void setMediaTrackingId(String value) { values[ClickstreamColumn.MEDIA_TRACKING_ID.ordinal()] = value; }

    public String mediaType() { return (String) values[ClickstreamColumn.MEDIA_TYPE.ordinal()]; }
    public void setMediaType(String value) { values[ClickstreamColumn.MEDIA_TYPE.ordinal()] = value; }

    public String mediaAction() { return (String) values[ClickstreamColumn.MEDIA_ACTION.ordinal()]; }
    public void setMediaAction(String value) { values[ClickstreamColumn.MEDIA_ACTION.ordinal()] = value; }

    public long mediaPosition() { return (long) values[ClickstreamColumn.MEDIA_POSITION.ordinal()]; }
    public void setMediaPosition(long value) { values[ClickstreamColumn.MEDIA_POSITION.ordinal()] = value; }

    public long mediaDuration() { return (long) values[ClickstreamColumn.MEDIA_DURATION.ordinal()]; }
    public void setMediaDuration(long value) { values[ClickstreamColumn.MEDIA_DURATION.ordinal()] = value; }

    public float mediaPercent() { return (float) values[ClickstreamColumn.MEDIA_PERCENT.ordinal()]; }
    public void setMediaPercent(float value) { values[ClickstreamColumn.MEDIA_PERCENT.ordinal()] = value; }

    public String errorType() { return (String) values[ClickstreamColumn.ERROR_TYPE.ordinal()]; }
    public void setErrorType(String value) { values[ClickstreamColumn.ERROR_TYPE.ordinal()] = value; }

    public long errorFingerprint() { return (long) values[ClickstreamColumn.ERROR_FINGERPRINT.ordinal()]; }
    public void setErrorFingerprint(long value) { values[ClickstreamColumn.ERROR_FINGERPRINT.ordinal()] = value; }

    public String errorScriptPath() { return (String) values[ClickstreamColumn.ERROR_SCRIPT_PATH.ordinal()]; }
    public void setErrorScriptPath(String value) { values[ClickstreamColumn.ERROR_SCRIPT_PATH.ordinal()] = value; }

    public long errorLine() { return (long) values[ClickstreamColumn.ERROR_LINE.ordinal()]; }
    public void setErrorLine(long value) { values[ClickstreamColumn.ERROR_LINE.ordinal()] = value; }

    public long errorColumn() { return (long) values[ClickstreamColumn.ERROR_COLUMN.ordinal()]; }
    public void setErrorColumn(long value) { values[ClickstreamColumn.ERROR_COLUMN.ordinal()] = value; }

    public long privacyGlobalPrivacyControl() { return (long) values[ClickstreamColumn.PRIVACY_GLOBAL_PRIVACY_CONTROL.ordinal()]; }
    public void setPrivacyGlobalPrivacyControl(long value) { values[ClickstreamColumn.PRIVACY_GLOBAL_PRIVACY_CONTROL.ordinal()] = value; }

    public long privacyDoNotTrack() { return (long) values[ClickstreamColumn.PRIVACY_DO_NOT_TRACK.ordinal()]; }
    public void setPrivacyDoNotTrack(long value) { values[ClickstreamColumn.PRIVACY_DO_NOT_TRACK.ordinal()] = value; }

    public String consentAnalytics() { return (String) values[ClickstreamColumn.CONSENT_ANALYTICS.ordinal()]; }
    public void setConsentAnalytics(String value) { values[ClickstreamColumn.CONSENT_ANALYTICS.ordinal()] = value; }

    public long consentUpdatedTs() { return (long) values[ClickstreamColumn.CONSENT_UPDATED_TS.ordinal()]; }
    public void setConsentUpdatedTs(long value) { values[ClickstreamColumn.CONSENT_UPDATED_TS.ordinal()] = value; }

    public String consentPolicyVersion() { return (String) values[ClickstreamColumn.CONSENT_POLICY_VERSION.ordinal()]; }
    public void setConsentPolicyVersion(String value) { values[ClickstreamColumn.CONSENT_POLICY_VERSION.ordinal()] = value; }

    public String sdkVersion() { return (String) values[ClickstreamColumn.SDK_VERSION.ordinal()]; }
    public void setSdkVersion(String value) { values[ClickstreamColumn.SDK_VERSION.ordinal()] = value; }

    public String sdkIntegrationMode() { return (String) values[ClickstreamColumn.SDK_INTEGRATION_MODE.ordinal()]; }
    public void setSdkIntegrationMode(String value) { values[ClickstreamColumn.SDK_INTEGRATION_MODE.ordinal()] = value; }

    public long batchEventCount() { return (long) values[ClickstreamColumn.BATCH_EVENT_COUNT.ordinal()]; }
    public void setBatchEventCount(long value) { values[ClickstreamColumn.BATCH_EVENT_COUNT.ordinal()] = value; }

    public long batchBytes() { return (long) values[ClickstreamColumn.BATCH_BYTES.ordinal()]; }
    public void setBatchBytes(long value) { values[ClickstreamColumn.BATCH_BYTES.ordinal()] = value; }

    public String deliveryTransport() { return (String) values[ClickstreamColumn.DELIVERY_TRANSPORT.ordinal()]; }
    public void setDeliveryTransport(String value) { values[ClickstreamColumn.DELIVERY_TRANSPORT.ordinal()] = value; }

    public long deliveryAttempt() { return (long) values[ClickstreamColumn.DELIVERY_ATTEMPT.ordinal()]; }
    public void setDeliveryAttempt(long value) { values[ClickstreamColumn.DELIVERY_ATTEMPT.ordinal()] = value; }

    public long deliveryFromIndexeddb() { return (long) values[ClickstreamColumn.DELIVERY_FROM_INDEXEDDB.ordinal()]; }
    public void setDeliveryFromIndexeddb(long value) { values[ClickstreamColumn.DELIVERY_FROM_INDEXEDDB.ordinal()] = value; }

    public long deliveryFinalFlush() { return (long) values[ClickstreamColumn.DELIVERY_FINAL_FLUSH.ordinal()]; }
    public void setDeliveryFinalFlush(long value) { values[ClickstreamColumn.DELIVERY_FINAL_FLUSH.ordinal()] = value; }

    public long requestId() { return (long) values[ClickstreamColumn.REQUEST_ID.ordinal()]; }
    public void setRequestId(long value) { values[ClickstreamColumn.REQUEST_ID.ordinal()] = value; }

    public String ipAddress() { return (String) values[ClickstreamColumn.IP_ADDRESS.ordinal()]; }
    public void setIpAddress(String value) { values[ClickstreamColumn.IP_ADDRESS.ordinal()] = value; }

    public String httpUserAgent() { return (String) values[ClickstreamColumn.HTTP_USER_AGENT.ordinal()]; }
    public void setHttpUserAgent(String value) { values[ClickstreamColumn.HTTP_USER_AGENT.ordinal()] = value; }

    public String httpOrigin() { return (String) values[ClickstreamColumn.HTTP_ORIGIN.ordinal()]; }
    public void setHttpOrigin(String value) { values[ClickstreamColumn.HTTP_ORIGIN.ordinal()] = value; }

    public String httpReferer() { return (String) values[ClickstreamColumn.HTTP_REFERER.ordinal()]; }
    public void setHttpReferer(String value) { values[ClickstreamColumn.HTTP_REFERER.ordinal()] = value; }

    public String httpAcceptLanguage() { return (String) values[ClickstreamColumn.HTTP_ACCEPT_LANGUAGE.ordinal()]; }
    public void setHttpAcceptLanguage(String value) { values[ClickstreamColumn.HTTP_ACCEPT_LANGUAGE.ordinal()] = value; }

    public String httpVersion() { return (String) values[ClickstreamColumn.HTTP_VERSION.ordinal()]; }
    public void setHttpVersion(String value) { values[ClickstreamColumn.HTTP_VERSION.ordinal()] = value; }

    public String httpContentType() { return (String) values[ClickstreamColumn.HTTP_CONTENT_TYPE.ordinal()]; }
    public void setHttpContentType(String value) { values[ClickstreamColumn.HTTP_CONTENT_TYPE.ordinal()] = value; }

    public long httpRequestBytes() { return (long) values[ClickstreamColumn.HTTP_REQUEST_BYTES.ordinal()]; }
    public void setHttpRequestBytes(long value) { values[ClickstreamColumn.HTTP_REQUEST_BYTES.ordinal()] = value; }

    public String httpSecChUa() { return (String) values[ClickstreamColumn.HTTP_SEC_CH_UA.ordinal()]; }
    public void setHttpSecChUa(String value) { values[ClickstreamColumn.HTTP_SEC_CH_UA.ordinal()] = value; }

    public String httpSecChUaMobile() { return (String) values[ClickstreamColumn.HTTP_SEC_CH_UA_MOBILE.ordinal()]; }
    public void setHttpSecChUaMobile(String value) { values[ClickstreamColumn.HTTP_SEC_CH_UA_MOBILE.ordinal()] = value; }

    public String httpSecChUaPlatform() { return (String) values[ClickstreamColumn.HTTP_SEC_CH_UA_PLATFORM.ordinal()]; }
    public void setHttpSecChUaPlatform(String value) { values[ClickstreamColumn.HTTP_SEC_CH_UA_PLATFORM.ordinal()] = value; }

    public String geoCountryCode() { return (String) values[ClickstreamColumn.GEO_COUNTRY_CODE.ordinal()]; }
    public void setGeoCountryCode(String value) { values[ClickstreamColumn.GEO_COUNTRY_CODE.ordinal()] = value; }

    public String geoRegion() { return (String) values[ClickstreamColumn.GEO_REGION.ordinal()]; }
    public void setGeoRegion(String value) { values[ClickstreamColumn.GEO_REGION.ordinal()] = value; }

    public String geoCity() { return (String) values[ClickstreamColumn.GEO_CITY.ordinal()]; }
    public void setGeoCity(String value) { values[ClickstreamColumn.GEO_CITY.ordinal()] = value; }

    public long networkAsn() { return (long) values[ClickstreamColumn.NETWORK_ASN.ordinal()]; }
    public void setNetworkAsn(long value) { values[ClickstreamColumn.NETWORK_ASN.ordinal()] = value; }

    public long networkIsDatacenter() { return (long) values[ClickstreamColumn.NETWORK_IS_DATACENTER.ordinal()]; }
    public void setNetworkIsDatacenter(long value) { values[ClickstreamColumn.NETWORK_IS_DATACENTER.ordinal()] = value; }

    public String networkProxyType() { return (String) values[ClickstreamColumn.NETWORK_PROXY_TYPE.ordinal()]; }
    public void setNetworkProxyType(String value) { values[ClickstreamColumn.NETWORK_PROXY_TYPE.ordinal()] = value; }

    public String customPayloadRaw() { return (String) values[ClickstreamColumn.CUSTOM_PAYLOAD_RAW.ordinal()]; }
    public void setCustomPayloadRaw(String value) { values[ClickstreamColumn.CUSTOM_PAYLOAD_RAW.ordinal()] = value; }

    public long customPayloadBytes() { return (long) values[ClickstreamColumn.CUSTOM_PAYLOAD_BYTES.ordinal()]; }
    public void setCustomPayloadBytes(long value) { values[ClickstreamColumn.CUSTOM_PAYLOAD_BYTES.ordinal()] = value; }

}
