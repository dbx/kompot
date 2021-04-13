package hu.dbx.kompot.ng.reports;

import hu.dbx.kompot.impl.DataHandling;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DataEntityLoggingTest {

    @Test
    public void testDataEntityPasswordLoggingFiltering() {
        final String s = "{\"username\": \"alma\", \"password\": \"kkorte\", \"partnerRef\": \"PA0001\"}";
        final String s1 = DataHandling.filterOutPassword(s);
        assertEquals("{\"username\": \"alma\", \"password\": <FILTERED>, \"partnerRef\": \"PA0001\"}", s1);
    }
}
