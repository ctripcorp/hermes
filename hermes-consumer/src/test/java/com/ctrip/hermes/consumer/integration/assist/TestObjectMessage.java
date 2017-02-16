package com.ctrip.hermes.consumer.integration.assist;

import java.util.Arrays;

public class TestObjectMessage {
	private String m_stringValue;

	private int m_intValue;

	private byte[] m_byteArrayValue;

	public TestObjectMessage(String stringValue, int intValue, byte[] arrayValue) {
		m_stringValue = stringValue;
		m_intValue = intValue;
		m_byteArrayValue = arrayValue;
	}

	public TestObjectMessage() {
	}

	public void setStringValue(String stringValue) {
		m_stringValue = stringValue;
	}

	public void setIntValue(int intValue) {
		m_intValue = intValue;
	}

	public void setByteArrayValue(byte[] byteArrayValue) {
		m_byteArrayValue = byteArrayValue;
	}

	public String getStringValue() {
		return m_stringValue;
	}

	public int getIntValue() {
		return m_intValue;
	}

	public byte[] getByteArrayValue() {
		return m_byteArrayValue;
	}

	@Override
	public String toString() {
		return "TestObjectMessage [m_stringValue=" + m_stringValue + ", m_intValue=" + m_intValue + ", m_byteArrayValue="
		      + Arrays.toString(m_byteArrayValue) + "]";
	}

	@Override
   public int hashCode() {
	   final int prime = 31;
	   int result = 1;
	   result = prime * result + Arrays.hashCode(m_byteArrayValue);
	   result = prime * result + m_intValue;
	   result = prime * result + ((m_stringValue == null) ? 0 : m_stringValue.hashCode());
	   return result;
   }

	@Override
   public boolean equals(Object obj) {
	   if (this == obj)
		   return true;
	   if (obj == null)
		   return false;
	   if (getClass() != obj.getClass())
		   return false;
	   TestObjectMessage other = (TestObjectMessage) obj;
	   if (!Arrays.equals(m_byteArrayValue, other.m_byteArrayValue))
		   return false;
	   if (m_intValue != other.m_intValue)
		   return false;
	   if (m_stringValue == null) {
		   if (other.m_stringValue != null)
			   return false;
	   } else if (!m_stringValue.equals(other.m_stringValue))
		   return false;
	   return true;
   }
}
