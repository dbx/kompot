package hu.dbx.kompot.ng.reports;

import hu.dbx.kompot.impl.DataHandling;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class DataEntityLoggingTest {

    @Test
    public void testDataEntityNoSensitiveData() {
        final String s = "{\"username\": \"alma\", \"pass1word\": \"kkorte\", \"partnerRef\": \"PA0001\"}";
        final String s1 = DataHandling.filterSensitiveDataOutFromMessage(s, Arrays.asList("password", "cardId"));
        assertEquals("{\"username\": \"alma\", \"pass1word\": \"kkorte\", \"partnerRef\": \"PA0001\"}", s1);
    }
    @Test
    public void testDataEntityPasswordLoggingFilteringWithQuotes() {
        final String s = "{\"username\": \"alma\", \"password\": \"kkorte\", \"partnerRef\": \"PA0001\"}";
        final String s1 = DataHandling.filterSensitiveDataOutFromMessage(s, Arrays.asList("password", "cardId"));
        assertEquals("{\"username\": \"alma\", \"password\": <FILTERED>, \"partnerRef\": \"PA0001\"}", s1);
    }

    @Test
    public void testDataEntityPasswordLoggingFilteringWithQuotesLastProperty() {
        final String s = "{\"username\": \"alma\", \"password\": \"kkorte\"}";
        final String s1 = DataHandling.filterSensitiveDataOutFromMessage(s, Arrays.asList("password", "cardId"));
        assertEquals("{\"username\": \"alma\", \"password\": <FILTERED>}", s1);
    }

    @Test
    public void testDataEntityPasswordLoggingFilteringWithoutQuotes() {
        final String s = "{\"username\": \"alma\", \"password\": null, \"partnerRef\": \"PA0001\"}";
        final String s1 = DataHandling.filterSensitiveDataOutFromMessage(s, Arrays.asList("password", "cardId"));
        assertEquals("{\"username\": \"alma\", \"password\": <FILTERED>, \"partnerRef\": \"PA0001\"}", s1);
    }

    @Test
    public void testDataEntityPasswordLoggingFilteringWithoutQuotesPasswordLastProperty() {
        final String s = "{\"username\": \"alma\", \"password\": null}";
        final String s1 = DataHandling.filterSensitiveDataOutFromMessage(s, Arrays.asList("password", "cardId"));
        assertEquals("{\"username\": \"alma\", \"password\": <FILTERED>}", s1);
    }

    @Test
    public void testDataEntityPasswordLoggingFilteringWithoutQuotesPasswordNotLastProperty() {
        final String s = "{\"username\": \"alma\", \"password\": null, \"partnerRef\": \"PA0001\"}";
        final String s1 = DataHandling.filterSensitiveDataOutFromMessage(s, Arrays.asList("password", "cardId"));
        assertEquals("{\"username\": \"alma\", \"password\": <FILTERED>, \"partnerRef\": \"PA0001\"}", s1);
    }

    @Test
    public void testDataEntityCardIdLoggingFilteringWithQuotes() {
        final String s = "{\"username\": \"alma\", \"cardId\": \"kkorte\", \"partnerRef\": \"PA0001\"}";
        final String s1 = DataHandling.filterSensitiveDataOutFromMessage(s, Arrays.asList("password", "cardId"));
        assertEquals("{\"username\": \"alma\", \"cardId\": <FILTERED>, \"partnerRef\": \"PA0001\"}", s1);
    }

    @Test
    public void testDataEntityCardIdLoggingFilteringWithQuotesLastProperty() {
        final String s = "{\"username\": \"alma\", \"cardId\": \"kkorte\"}";
        final String s1 = DataHandling.filterSensitiveDataOutFromMessage(s, Arrays.asList("password", "cardId"));
        assertEquals("{\"username\": \"alma\", \"cardId\": <FILTERED>}", s1);
    }

    @Test
    public void testDataEntityCardIdLoggingFilteringWithoutQuotes() {
        final String s = "{\"username\": \"alma\", \"cardId\": null, \"partnerRef\": \"PA0001\"}";
        final String s1 = DataHandling.filterSensitiveDataOutFromMessage(s, Arrays.asList("password", "cardId"));
        assertEquals("{\"username\": \"alma\", \"cardId\": <FILTERED>, \"partnerRef\": \"PA0001\"}", s1);
    }

    @Test
    public void testDataEntityCardIdLoggingFilteringWithoutQuotesCardIdLastProperty() {
        final String s = "{\"username\": \"alma\", \"cardId\": null}";
        final String s1 = DataHandling.filterSensitiveDataOutFromMessage(s, Arrays.asList("password", "cardId"));
        assertEquals("{\"username\": \"alma\", \"cardId\": <FILTERED>}", s1);
    }

    @Test
    public void testDataEntityCardIdLoggingFilteringWithoutQuotesCardIdNotLastProperty() {
        final String s = "{\"username\": \"alma\", \"cardId\": null, \"partnerRef\": \"PA0001\"}";
        final String s1 = DataHandling.filterSensitiveDataOutFromMessage(s, Arrays.asList("password", "cardId"));
        assertEquals("{\"username\": \"alma\", \"cardId\": <FILTERED>, \"partnerRef\": \"PA0001\"}", s1);
    }

    @Test
    public void testDataEntityPasswordAndCardIdLoggingFilteringWithQuotes() {
        final String s = "{\"username\": \"alma\", \"cardId\": \"kkorte\", \"password\": \"kkorte\", \"partnerRef\": \"PA0001\"}";
        final String s1 = DataHandling.filterSensitiveDataOutFromMessage(s, Arrays.asList("password", "cardId"));
        assertEquals("{\"username\": \"alma\", \"cardId\": <FILTERED>, \"password\": <FILTERED>, \"partnerRef\": \"PA0001\"}", s1);
    }

    @Test
    public void testDataEntityPasswordAndCardIdLoggingFilteringWithoutQuotes() {
        final String s = "{\"username\": \"alma\", \"cardId\": null, \"password\": null, \"partnerRef\": \"PA0001\"}";
        final String s1 = DataHandling.filterSensitiveDataOutFromMessage(s, Arrays.asList("password", "cardId"));
        assertEquals("{\"username\": \"alma\", \"cardId\": <FILTERED>, \"password\": <FILTERED>, \"partnerRef\": \"PA0001\"}", s1);
    }

    @Test
    public void testDataEntityPasswordAndCardIdLoggingFilteringWithoutQuotesCardIdNotLastProperty() {
        final String s = "{\"username\": \"alma\", \"cardId\": null, \"password\": null, \"partnerRef\": \"PA0001\"}";
        final String s1 = DataHandling.filterSensitiveDataOutFromMessage(s, Arrays.asList("password", "cardId"));
        assertEquals("{\"username\": \"alma\", \"cardId\": <FILTERED>, \"password\": <FILTERED>, \"partnerRef\": \"PA0001\"}", s1);
    }

}
