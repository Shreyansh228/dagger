package com.gojek.daggers.postProcessors.external.http;

import com.gojek.daggers.postProcessors.external.ExternalMetricConfig;
import com.gojek.daggers.postProcessors.external.SchemaConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpStreamDecoratorTest {
    @Mock
    SchemaConfig schemaConfig;

    @Mock
    private HttpSourceConfig httpSourceConfig;

    @Mock
    private ExternalMetricConfig externalMetricConfig;


    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void canDecorateHttpAsync() {
        HttpStreamDecorator httpStreamDecorator = new HttpStreamDecorator(httpSourceConfig, externalMetricConfig, schemaConfig);
        assertTrue(httpStreamDecorator.canDecorate());
    }

    @Test
    public void shouldNotDecorateOtherThanHttpAsync() {
        HttpStreamDecorator httpStreamDecorator = new HttpStreamDecorator(null, externalMetricConfig, schemaConfig);
        assertFalse(httpStreamDecorator.canDecorate());
    }
}