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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.junit.Test;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;

public class FixedFileInputPropertiesTest {

	/**
	 * Checks forms are filled with required widgets
	 */
	@Test
	public void testSetupLayout() {
		FixedFileInputProperties properties = new FixedFileInputProperties("root");
		properties.schema.init();

		properties.setupLayout();

		Form main = properties.getForm(Form.MAIN);

		Collection<Widget> mainWidgets = main.getWidgets();
		assertThat(mainWidgets, hasSize(7));

		Widget schemaWidget = main.getWidget("schema");
		assertThat(schemaWidget, notNullValue());

		Widget fileWidget = main.getWidget("filename");
		assertThat(fileWidget, notNullValue());

		Widget guessSchemaWidget = main.getWidget("guessSchema");
		assertThat(guessSchemaWidget, notNullValue());
	}

	/**
	 * Checks default values are set correctly
	 */
	@Test
	public void testSetupProperties() {
		FixedFileInputProperties properties = new FixedFileInputProperties("root");
		properties.setupProperties();
	}

	/**
	 * Checks initial layout
	 */
	@Test
	public void testRefreshLayout() {
		FixedFileInputProperties properties = new FixedFileInputProperties("root");
		properties.init();

		properties.refreshLayout(properties.getForm(Form.MAIN));

		boolean schemaHidden = properties.getForm(Form.MAIN).getWidget("schema").isHidden();
		assertFalse(schemaHidden);

		boolean filenameHidden = properties.getForm(Form.MAIN).getWidget("filename").isHidden();
		assertFalse(filenameHidden);

		boolean guessSchemaHidden = properties.getForm(Form.MAIN).getWidget("guessSchema").isHidden();
		assertFalse(guessSchemaHidden);
	}
}
