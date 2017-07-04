// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.fixedfile.tfixedFileInput;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;
import static org.talend.components.api.component.ComponentDefinition.RETURN_ERROR_MESSAGE_PROP;
import static org.talend.components.api.component.ComponentDefinition.RETURN_TOTAL_RECORD_COUNT_PROP;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.component.runtime.ExecutionEngine;
import org.talend.daikon.exception.TalendRuntimeException;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.runtime.RuntimeInfo;

public class FixedFileInputDefinitionTest {

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	@Test
	public void testGetFamilies() {
		FixedFileInputDefinition definition = new FixedFileInputDefinition();
		String[] actual = definition.getFamilies();

		assertThat(Arrays.asList(actual), contains("File/Input"));
	}

	@Test
	public void testGetPropertyClass() {
		FixedFileInputDefinition definition = new FixedFileInputDefinition();
		Class<?> propertyClass = definition.getPropertyClass();
		String canonicalName = propertyClass.getCanonicalName();

		assertThat(canonicalName, equalTo("org.talend.components.fixedfile.tfixedFileInput.FixedFileInputProperties"));
	}

	@Test
	public void testGetReturnProperties() {
		FixedFileInputDefinition definition = new FixedFileInputDefinition();
		Property[] returnProperties = definition.getReturnProperties();
		List<Property> propertyList = Arrays.asList(returnProperties);

		assertThat(propertyList, hasSize(3));
		assertTrue(propertyList.contains(RETURN_TOTAL_RECORD_COUNT_PROP));
		assertTrue(propertyList.contains(RETURN_ERROR_MESSAGE_PROP));
	}
	
	@Test
	public void testGetRuntimeInfo() {
		FixedFileInputDefinition definition = new FixedFileInputDefinition();
		RuntimeInfo runtimeInfo = definition.getRuntimeInfo(ExecutionEngine.DI, null, ConnectorTopology.OUTGOING);
		String runtimeClassName = runtimeInfo.getRuntimeClassName();
		assertThat(runtimeClassName, equalTo("org.talend.components.fixedfile.runtime.reader.FixedFileInputSource"));
	}

	@Test
	public void testGetRuntimeInfoWrongEngine() {
		FixedFileInputDefinition definition = new FixedFileInputDefinition();
		thrown.expect(TalendRuntimeException.class);
		thrown.expectMessage("WRONG_EXECUTION_ENGINE:{component=FixedFileInput, requested=DI_SPARK_STREAMING, available=[DI, BEAM]}");
		definition.getRuntimeInfo(ExecutionEngine.DI_SPARK_STREAMING, null, ConnectorTopology.OUTGOING);
	}

	@Test
	public void testGetRuntimeInfoWrongTopology() {
		FixedFileInputDefinition definition = new FixedFileInputDefinition();
		thrown.expect(TalendRuntimeException.class);
		thrown.expectMessage("WRONG_CONNECTOR:{component=FixedFileInput}");
		definition.getRuntimeInfo(ExecutionEngine.DI, null, ConnectorTopology.INCOMING);
	}
	
	@Test
	public void testGetSupportedConnectorTopologies() {
		FixedFileInputDefinition definition = new FixedFileInputDefinition();
		Set<ConnectorTopology> connectorTopologies = definition.getSupportedConnectorTopologies();
		
		assertThat(connectorTopologies, contains(ConnectorTopology.OUTGOING));
		assertThat(connectorTopologies, not((contains(ConnectorTopology.INCOMING,ConnectorTopology.NONE,ConnectorTopology.INCOMING_AND_OUTGOING))));
	}
	
}