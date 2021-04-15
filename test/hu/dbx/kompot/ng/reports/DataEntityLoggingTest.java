package hu.dbx.kompot.ng.reports;

import hu.dbx.kompot.impl.DataHandling;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DataEntityLoggingTest {

    @Test
    public void testDataEntityPasswordLoggingFilteringWithQuotes() {
        final String s = "{\"username\": \"alma\", \"password\": \"kkorte\", \"partnerRef\": \"PA0001\"}";
        final String s1 = DataHandling.filterOutPassword(s);
        assertEquals("{\"username\": \"alma\", \"password\": <FILTERED>, \"partnerRef\": \"PA0001\"}", s1);
    }

    @Test
    public void testDataEntityPasswordLoggingFilteringWithQuotesLastProperty() {
        final String s = "{\"username\": \"alma\", \"password\": \"kkorte\"}";
        final String s1 = DataHandling.filterOutPassword(s);
        assertEquals("{\"username\": \"alma\", \"password\": <FILTERED>}", s1);
    }

    @Test
    public void testDataEntityPasswordLoggingFilteringWithoutQuotes() {
        final String s = "{\"username\": \"alma\", \"password\": null, \"partnerRef\": \"PA0001\"}";
        final String s1 = DataHandling.filterOutPassword(s);
        assertEquals("{\"username\": \"alma\", \"password\": <FILTERED>, \"partnerRef\": \"PA0001\"}", s1);
    }

    @Test
    public void testDataEntityPasswordLoggingFilteringWithoutQuotesPasswordLastProperty() {
        final String s = "{\"username\": \"alma\", \"password\": null}";
        final String s1 = DataHandling.filterOutPassword(s);
        assertEquals("{\"username\": \"alma\", \"password\": <FILTERED>}", s1);
    }

    @Test
    public void testDataEntityPasswordLoggingFilteringWithoutQuotesPasswordNotLastProperty() {
        final String s = "{\"username\": \"alma\", \"password\": null, \"partnerRef\": \"PA0001\"}";
        final String s1 = DataHandling.filterOutPassword(s);
        assertEquals("{\"username\": \"alma\", \"password\": <FILTERED>, \"partnerRef\": \"PA0001\"}", s1);
    }
}
