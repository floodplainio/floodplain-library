package com.dexels.navajo.document.test;

import com.dexels.navajo.document.Navajo;
import com.dexels.navajo.document.NavajoFactory;
import com.dexels.navajo.document.Selection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSelection {

	private NavajoDocumentTestFicture fixture = new NavajoDocumentTestFicture();
	private Navajo testDoc;

	@Before
	public void setUp() {
		fixture.setUp();
		testDoc = fixture.testDoc;
	}

	@After
	public void tearDown() {
		fixture.tearDown();
	}

	@Test
	public void testSetSelected() {

		Selection selection = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", true);
		boolean b1 = true;
		selection.setSelected(b1);
		Assert.assertEquals(selection.isSelected(), true);
	}

	@Test
	public void testCreate() {
		Selection selectionRet = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", true);
		Assert.assertEquals(selectionRet.getName(), "firstselection");
		Assert.assertEquals(selectionRet.getValue(), "0");
		Assert.assertEquals(selectionRet.isSelected(), true);
	}

	@Test
	public void testCreateDummy() {
		Selection selectionRet = NavajoFactory.getInstance()
				.createDummySelection();
		Assert.assertEquals("___DUMMY_ELEMENT___", selectionRet.getValue());
	}

	@Test
	public void testGetName() {
		Selection selectionRet = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", true);
		Assert.assertEquals(selectionRet.getName(), "firstselection");
	}

	@Test
	public void testGetValue() {
		Selection selectionRet = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", true);
		Assert.assertEquals(selectionRet.getValue(), "0");
	}

	@Test
	public void testIsSelected() {
		Selection selectionRet1 = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", true);
		Selection selectionRet2 = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", false);
		Assert.assertTrue(selectionRet1.isSelected());
		Assert.assertTrue(!selectionRet2.isSelected());
	}

	@Test
	public void testSetName() {
		Selection selectionRet = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", true);
		selectionRet.setName("another");
		Assert.assertEquals(selectionRet.getName(), "another");
	}

	@Test
	public void testSetValue() {
		Selection selectionRet = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", true);
		selectionRet.setValue("1");
		Assert.assertEquals(selectionRet.getValue(), "1");
	}

	@Test
	public void testIsSelectedWithInteger0() {
		Selection selectionRet = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", 1);
		selectionRet.setValue("1");
		Assert.assertTrue(selectionRet.isSelected());
	}

	@Test
	public void testIsSelectedWithInteger1() {
		Selection selectionRet = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", 0);
		selectionRet.setValue("1");
		Assert.assertTrue(!selectionRet.isSelected());
	}

	@Test
	public void testIsSelectedWithInteger2() {
		Selection selectionRet = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", 11);
		selectionRet.setValue("1");
		Assert.assertTrue(selectionRet.isSelected());
	}

	@Test
	public void testIsSelectedWithObject0() {
		Selection selectionRet = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", Integer.valueOf(1));
		selectionRet.setValue("1");
		Assert.assertTrue(selectionRet.isSelected());
	}

	@Test
	public void testIsSelectedWithObject1() {
		Selection selectionRet = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", Integer.valueOf(0));
		selectionRet.setValue("1");
		Assert.assertTrue(!selectionRet.isSelected());
	}

	@Test
	public void testIsSelectedWithObject2() {
		Selection selectionRet = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", Integer.valueOf(11));
		selectionRet.setValue("1");
		Assert.assertTrue(selectionRet.isSelected());
	}

	@Test
	public void testIsSelectedWithObject3() {
		Selection selectionRet = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", Boolean.valueOf(true));
		selectionRet.setValue("1");
		Assert.assertTrue(selectionRet.isSelected());
	}

	@Test
	public void testIsSelectedWithObject4() {
		Selection selectionRet = NavajoFactory.getInstance().createSelection(
				testDoc, "firstselection", "0", Boolean.valueOf(false));
		selectionRet.setValue("1");
		Assert.assertTrue(!selectionRet.isSelected());
	}
}
