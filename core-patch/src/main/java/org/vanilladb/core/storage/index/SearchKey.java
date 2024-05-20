/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.storage.index;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.sql.Types;
import java.sql.Types.*;


import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.CosineFn;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
/**
 * A SearchKey represents an array of constants for a list of indexed fields.
 */
public class SearchKey implements Comparable<SearchKey> {

	private Constant[] vals;
	private boolean hasHashCode;
	private int hashCode;
	private DistanceFn distFn;
	private boolean isQueriedDistFn;

	/**
	 * Constructs from the given values for the specified field names.
	 * 
	 * @param indexedFields
	 *            the list of field names for the key
	 * @param fldValMap
	 *            the map stores the actual values
	 * @throws NullPointerException
	 *             if there is a missing value
	 */
	public SearchKey(List<String> indexedFields, Map<String, Constant> fldValMap) {
		vals = new Constant[indexedFields.size()];
		Iterator<String> fldNameIter = indexedFields.iterator();
		String fldName;

		for (int i = 0; i < vals.length; i++) {
			fldName = fldNameIter.next();
			vals[i] = fldValMap.get(fldName);
			if (vals[i] == null)
				throw new NullPointerException("there is no value for '" + fldName + "'");
		}
		
		distFn = new EuclideanFn("dummy");
		isQueriedDistFn = false;
	}

	public SearchKey(Constant... constants) {
		vals = Arrays.copyOf(constants, constants.length);
		distFn = new EuclideanFn("dummy");
		isQueriedDistFn = false;
	}
	
	public int length() {
		return vals.length;
	}
	
	public Constant get(int index) {
		return vals[index];
	}
	
	@Override
	public String toString() {
		return Arrays.toString(vals);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		
		if (this == obj)
			return true;
		
		if (!obj.getClass().equals(SearchKey.class))
			return false;
		
		SearchKey targetKey = (SearchKey) obj;
		
		if (vals.length != targetKey.vals.length)
			return false;
		
		for (int i = 0; i < vals.length; i++) {
			if (!vals[i].equals(targetKey.vals[i]))
				return false;
		}
		
		return true;
	}
	
	@Override
	public int hashCode() {
		if (hasHashCode)
			return hashCode;
		
		// Generate the hash code
		hashCode = 37;
		for (Constant val : vals)
			hashCode = 37 * hashCode + val.hashCode();
		hasHashCode = true;
		
		return hashCode;
	}
	
	@Override
	public int compareTo(SearchKey targetKey) {
		// It will not compare the key without the same length
		if (vals.length != targetKey.vals.length)
			throw new IllegalArgumentException("The compared key does not have the same length");
		
		// Compare the value one by one
		int comResult;
		for (int i = 0; i < vals.length; i++) {
			comResult = vals[i].compareTo(targetKey.vals[i]);
			
			if (comResult < 0)
				return -1;
			else if (comResult > 0)
				return 1;
		}
		return 0;
	}
	
	public double vectorDistance(SearchKey targetKey) {
		// It will not compare the key without the same length
		if (this.vals.length != targetKey.vals.length)
			throw new IllegalArgumentException("The compared key does not have the same length");
		
		// Check if VectorConstant is available on the key
		
		VectorConstant lhs = null, rhs = null;
		for(int i = 0; i < this.vals.length; i++) {
			if (this.vals[i].getType().getSqlType() == Types.ARRAY) lhs = (VectorConstant) this.vals[i];
			if (targetKey.vals[i].getType().getSqlType() == Types.ARRAY) rhs = (VectorConstant) targetKey.vals[i];
		}
		
		if (lhs == null && rhs == null) throw new IllegalArgumentException("Both compared key does not have VectorConstant field");
		if (lhs == null) throw new IllegalArgumentException("The left key does not have VectorConstant field");

		if (rhs == null) throw new IllegalArgumentException("The right key does not have VectorConstant field");
		
		// Query the Vector into 
		this.distFn.setQueryVector(lhs);
		return this.distFn.distance(rhs);
	}
}
