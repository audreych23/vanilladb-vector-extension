/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
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
package org.vanilladb.core.storage.index.ivf;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.VectorConstant;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.record.RecordPage;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

/**
 * A Ivfflat index implementation of {@link Index}. the 0th indexname is the centroid files
 * the [1, n + 1] is the data files, where n is the number of clusters 
 */
public class IVFIndex extends Index {
	
	/**
	 * A field name of the schema of index records.
	 */
	private static final String SCHEMA_KEY = "key", SCHEMA_RID_BLOCK = "block",
			SCHEMA_RID_ID = "id";

//	public static final int NUM_BUCKETS;
//
//	static {
//		NUM_BUCKETS = CoreProperties.getLoader().getPropertyAsInteger(
//				IVFIndex.class.getName() + ".NUM_BUCKETS", 100);
//	}

	public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
//		int rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(schema(keyType));
//		return (totRecs / rpb) / NUM_BUCKETS;
		return 0;
	}
	
	private static String keyFieldName(int index) {
		return SCHEMA_KEY + index;
	}

	/**
	 * Parameter to control IVF re-cluster and search.
	 * 
	 * (?) : not yet implemented...
	 */
	// (?) Search probe to find the best vector ?
	private static final int N_PROBE;
	static {
		N_PROBE = CoreProperties.getLoader().getPropertyAsInteger(
				IVFIndex.class.getName() + ".N_PROBE", 3);
	}
	// Number of update to trigger re-cluster (K-means) procedure
	private static final int N_CHANGES_BEFORE_RECLUSTER;
	static {
		N_CHANGES_BEFORE_RECLUSTER = CoreProperties.getLoader().getPropertyAsInteger(
				IVFIndex.class.getName() + ".N_CHANGES_BEFORE_RECLUSTER", 4096);
	}
	// Maximum number of iteration on re-cluster (K-means) procedure
	private static final int MAX_ITER_RECLUSTER;
	static {
		MAX_ITER_RECLUSTER = CoreProperties.getLoader().getPropertyAsInteger(
				IVFIndex.class.getName() + ".MAX_ITER_RECLUSTER", 32);
	}
	
	private static final int NUM_CLUSTERS;
	static {
		NUM_CLUSTERS = CoreProperties.getLoader().getPropertyAsInteger(
				IVFIndex.class.getName() + ".NUM_CLUSTERS", 32);
	}

		
	/**
	 * Returns the schema of the index records.
	 * 
	 * @param fldType
	 *            the type of the indexed field
	 * 
	 * @return the schema of the index records
	 */
	private static Schema schema(SearchKeyType keyType) {
		Schema sch = new Schema();
		for (int i = 0; i < keyType.length(); i++)
			sch.addField(keyFieldName(i), keyType.get(i));
		sch.addField(SCHEMA_RID_BLOCK, BIGINT);
		sch.addField(SCHEMA_RID_ID, INTEGER);
		return sch;
	}
	
	// should we make a new schema since the centroid page schema is different?
	
	private SearchKey searchKey;
	private RecordFile rf, rfCentroid;
	private boolean isBeforeFirsted;
	private SortedMap<Double, Integer> currentNearestCentroids;
	private int vecDim;
	
    private static final Random random = new Random();
    
	@Override
	public void preLoadToMemory() {
//		int num_centroid = 0;
//		// DO: Added .tbl
//		String tblname = ii.indexName() + 0 + ".tbl";
//		long size = fileSize(tblname);
//		BlockId blk;
//		// Open Centroid record file
//		TableInfo ti = new TableInfo(tblname, schema(keyType));
//		this.rfCentroid = ti.open(tx, false);
//		// DO: Added BeforeFirst
//		rfCentroid.beforeFirst();
//		// Calculate how many number of centroid we have
//		while (rfCentroid.next()) num_centroid++;
		
		// Shift to indexed 1.
		// num_clusters = num_centroids
		// Iterate starting from 0 because we also want to preLoad the cluster page
		for (int i = 0; i < (NUM_CLUSTERS + 1); i++) {
			String tblname = ii.indexName() + i + ".tbl";
			long size = fileSize(tblname);
			BlockId blk;
			for (int j = 0; j < size; j++) {
				blk = new BlockId(tblname, j);
				tx.bufferMgr().pin(blk);
			}
		}
	}
	/**
	 * Opens an IVFIndex for the specified index. Create a new record file 
	 * for the centroid
	 * 
	 * @param ii
	 *            the information of this index
	 * @param keyType
	 *            the type of the search key
	 * @param tx
	 *            the calling transaction
	 */
	public IVFIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
		super(ii, keyType, tx);
		// get vector dimension 
		vecDim = (keyType.get(keyType.findVectorIdx())).getArgument();
		String tblname = ii.indexName() + 0;
		TableInfo ti = new TableInfo(tblname, schema(keyType));
		this.rfCentroid = ti.open(tx, false);
		
		if (rfCentroid.fileSize() == 0) 
			RecordFile.formatFileHeader(ti.fileName(), tx);
		rfCentroid.beforeFirst();
		// keep inserting and make the new page dpeending on how many clusters you have rn
		for (int m = 0; m < NUM_CLUSTERS; ++m) {
			rfCentroid.insert();
			for (int i = 0; i < keyType.length(); ++i) {
				// ASSUME that the length = 0
				rfCentroid.setVal(keyFieldName(i), VectorConstant.zeros(vecDim));
			}
			// some dummy value
			rfCentroid.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(0));
			// dummy to put which centroid it is
			rfCentroid.setVal(SCHEMA_RID_ID, new IntegerConstant(m + 1));
		}
		rfCentroid.close();
		// log the logical operation ends
		
		
		tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), null, 0, 0);
		
	}


	
	/**
	 * Positions the index before the first index record having the specified
	 * search key. The method hashes the search key to determine the bucket, and
	 * then opens a {@link RecordFile} on the file corresponding to the bucket.
	 * The record file for the previous bucket (if any) is closed.
	 * 
	 * Important: Centroid List at file 0 and Data List start at file 1.
	 * 
	 * @see Index#beforeFirst(SearchRange)
	 */
	@Override
	public void beforeFirst(SearchRange searchRange) {
		close();
		// support the equality query only
		if (!searchRange.isSingleValue())
			throw new UnsupportedOperationException();

		this.searchKey = searchRange.asSearchKey();
		
		// open the nearest centroid
		currentNearestCentroids = getNearestCentroids(searchKey);
		// Get the nearest one for now
		Double firstKey = currentNearestCentroids.keySet().iterator().next();
		int nearestCentroidId = currentNearestCentroids.get(firstKey); 
		
		String tblname = ii.indexName() + nearestCentroidId; 
		
		TableInfo ti = new TableInfo(tblname, schema(keyType));

		// the underlying record file should not perform logging
		this.rf = ti.open(tx, false);

		// This is the first time that this has been opened, it means there are no clusters yet
		// we set the new cluster point with the current point
		// should only be called when it needs inserting
		// initialize the file header if needed
		if (rf.fileSize() == 0) {
			RecordFile.formatFileHeader(ti.fileName(), tx);
			// iterate rfCentroid to change the centroid vector
			rfCentroid.beforeFirst();
			while(rfCentroid.next()) {
				Integer currentCentroidId = (Integer) rfCentroid.getVal(SCHEMA_RID_ID).asJavaVal();
				// this method is kinda stupid
				if (currentCentroidId == nearestCentroidId) {
					// let's hope there are actually 1 index only which is 0
					// Set the centroid vector into the page 
					rfCentroid.setVal(keyFieldName(0), searchKey.get(0));
				}
			}
		}
		rf.beforeFirst();
		
		isBeforeFirsted = true;
	}
	
	/**
	 * Iterate through the centroids list and calculate Euclidean distance of the vector 
	 * from the input {@link SearchKey} with the key which each centroid has.
	 * 
	 * Return a list of centroid with size of {@link n_nearest}
	 * 
	 * Important: 
	 * Centroid List at file indexed 0
	 * Data List start at file indexed 1
	 * 
	 * @see Index#beforeFirst(SearchRange)
	 */
	private SortedMap<Double, Integer> getNearestCentroids(SearchKey searchKey) {
		
		// Checking if the rfCentroid has been opened or not, if not we re-open the file
		if (this.rfCentroid == null) {
			String tblname = ii.indexName() + 0; // centroid list listed at file indexed 0
			TableInfo ti = new TableInfo(tblname, schema(keyType));
			// the underlying record file should not perform logging
			this.rfCentroid = ti.open(tx, false);
			// initialize the file header if needed
			if (rfCentroid.fileSize() == 0)
				RecordFile.formatFileHeader(ti.fileName(), tx);
		}

		rfCentroid.beforeFirst();
		
		// search the nearest by iterating through the centroid list
	    SortedMap<Double, Integer> mapDistCentroidId = new TreeMap<Double, Integer>();
		// index data file start from 1
		while (rfCentroid.next()) {
			Constant dummyCentroidId = rfCentroid.getVal(SCHEMA_RID_ID);
			Integer centroidId = (Integer) dummyCentroidId.asJavaVal();
			// getKey(RecordFile) returns the searchKey 
			double curDistance = searchKey.vectorDistance(getKey(rfCentroid));
			mapDistCentroidId.put(curDistance, centroidId);
		}
		return mapDistCentroidId;
	}
	

	
	/**
	 * Moves to the next index record having the search key.
	 * 
	 * @see Index#next()
	 */
	@Override
	public boolean next() {
		if (!isBeforeFirsted)
			throw new IllegalStateException("You must call beforeFirst() before iterating index '"
					+ ii.indexName() + "'");
		
		// TODO: Implement probing search through nearest centroid to yield better result.
		Iterator<Double> currentNearestCentroidsIterator = currentNearestCentroids.keySet().iterator();
		int dataPageOpened = 0;
		while (true) { 
			while (rf.next()) return true;
			dataPageOpened++;
			// assume we're only opening 1 currently
			if ((!currentNearestCentroidsIterator.hasNext()) || dataPageOpened == 1) break;
			Double currentMapKey = currentNearestCentroidsIterator.next();
			int centroidId = currentNearestCentroids.get(currentMapKey); // for now get the nearest ones
			
			String tblname = ii.indexName() + centroidId; 
			
			TableInfo ti = new TableInfo(tblname, schema(keyType));
			
			// the underlying record file should not perform logging
			this.rf = ti.open(tx, false);

			// initialize the file header if needed
			if (rf.fileSize() == 0)
				// kinda broken here? actually is fine nvm
				RecordFile.formatFileHeader(ti.fileName(), tx);
			rf.beforeFirst();
		}
		return false;
	}

	/**
	 * Retrieves the data record ID from the current index record.
	 * 
	 * @see Index#getDataRecordId()
	 */
	@Override
	public RecordId getDataRecordId() {
		long blkNum = (Long) rf.getVal(SCHEMA_RID_BLOCK).asJavaVal();
		int id = (Integer) rf.getVal(SCHEMA_RID_ID).asJavaVal();
		return new RecordId(new BlockId(dataFileName, blkNum), id);
	}
	
	/**
	 * Retrieves the 
	 * @return
	 */
	private RecordId getCentroidRecordId() {
		long blkNum = (Long) rfCentroid.getVal(SCHEMA_RID_BLOCK).asJavaVal();
		int id = (Integer) rfCentroid.getVal(SCHEMA_RID_ID).asJavaVal();
		return new RecordId(new BlockId(dataFileName, blkNum), id);
	}

	/**
	 * Inserts a new index record into this index.
	 * 
	 * @see Index#insert(SearchKey, RecordId, boolean)
	 */
	@Override
	public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		// search the position
		beforeFirst(new SearchRange(key));
		
		// log the logical operation starts
		if (doLogicalLogging)
			tx.recoveryMgr().logLogicalStart();
		
		// insert the data
		rf.insert();
		for (int i = 0; i < keyType.length(); i++)
			rf.setVal(keyFieldName(i), key.get(i));
		rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block()
				.number()));
		rf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));
		
		// log the logical operation ends
		if (doLogicalLogging)
			tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key,
					dataRecordId.block().number(), dataRecordId.id());
		
		// TODO: Increment total_changes
	}

	/**
	 * Deletes the specified index record.
	 * 
	 * @see Index#delete(SearchKey, RecordId, boolean)
	 */
	@Override
	public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		// search the position
		beforeFirst(new SearchRange(key));
		
		// log the logical operation starts
		if (doLogicalLogging)
			tx.recoveryMgr().logLogicalStart();
		
		// delete the specified entry
		while (next())
			if (getDataRecordId().equals(dataRecordId)) {
				rf.delete();
				// TODO: Increment total_changes
				break;
			}
		
		// log the logical operation ends
		if (doLogicalLogging)
			tx.recoveryMgr().logIndexDeletionEnd(ii.indexName(), key,
					dataRecordId.block().number(), dataRecordId.id());
		
		// TODO: Increment total_changes
	}

	/**
	 * Closes the index by closing the current table scan.
	 * 
	 * @see Index#close()
	 */
	@Override
	public void close() {
		if (rf != null)
			rf.close();
		if (rfCentroid != null) {
			rfCentroid.close();
		}
	}

	private long fileSize(String fileName) {
		tx.concurrencyMgr().readFile(fileName);
		return VanillaDb.fileMgr().size(fileName);
	}
	
	private SearchKey getKey() {
		Constant[] vals = new Constant[keyType.length()];
		for (int i = 0; i < vals.length; i++)
			vals[i] = rf.getVal(keyFieldName(i));
		return new SearchKey(vals);
	}
	
	private SearchKey getKey(RecordFile recordFile) {
		Constant[] vals = new Constant[keyType.length()];
		for (int i = 0; i < vals.length; i++)
			vals[i] = recordFile.getVal(keyFieldName(i));
		return new SearchKey(vals);
	}
}
	
//	/**
//	 * Get the Nearest centroid id given the record. Euclidean distance are used to calculate 
//	 * the record distance with all the cluster.
//	 * 
//	 * @param record: Record
//	 * @return centroidId: Int
//	 */
//	public int getNearestCentroid(Record record) {
//		if (rfCentroid == null) {
//			String tblname = ii.indexName() + 0;
//			TableInfo ti = new TableInfo(tblname, schema(keyType));
//	
//			// the underlying record file should not perform logging
//			this.rfCentroid = ti.open(tx, false);
//	
//			// initialize the file header if needed
//			if (rfCentroid.fileSize() == 0)
//				RecordFile.formatFileHeader(ti.fileName(), tx);
//		}
//		
//		// WARNING!! : check first if the vector type exist
//		// We can use a table 
//		// Other implementations: just open the index one by one
//		for (fldName in record.fields()) {
//			if (fldName.type() == TYPES.ARRAY)
//				VectorConstant embFieldData = record.getVal(fldName);
//			else
//				throw IllegalArgumentException("There are no vector fields ");
//		}
//				
//		DistanFn dist = init EuclideanDistance(embFieldData);
//		double minDistance = Double.MAX_VALUE;
//		int nearestCentroid = 0;
//		
//		int centroidId = 0;
//		while(rfCentroid.next()) {
//			// should be ???
//			// get centroid vector data
//			Constant val = rfCentroid.getVal(keyFieldName(0));
//			// current distance to each cluster
//			double currentDistance = dist.distance(val);
//			// update minimum distance
//			if (currentDistance < minDistance) {
//				minDistance = currentDistance;
//				nearestCentroid = centroidId;
//			}
//			centroidId++;
//		}
//		return nearestCentroid;
//		
//	}
//	
//	/**
//	 * 
//	 * @param records
//	 * @param k
//	 * @param distance
//	 * @param maxIterations
//	 * @return
//	 */
//	
//	// TODO: Implement the full k-means / update cluster
//	// TODO: Update on every 4069 changes
//	public Map<VectorConstant, List<Record>> fit(List<Record> records, int k, Distance distance, int maxIterations) {
//
//		// TODO: do random centroid assignment / use the old ones?
//	    // randomCentroids(records, k);
//	    // iterate for a pre-defined number of times
//
//	    for (int i = 0; i < maxIterations; i++) {
//	        boolean isLastIteration = (i == maxIterations - 1);
//	        // Optimization : Delete whole data record in the index in O(1)? 
//	        // This one has 2 ways we can get records 
//	        // 1. we iterate the whole index page and then literally make a new one one by one 
//	        // 2. open a table (but this might not be a good idea because opening a table should be only when inserting?)
//	        for (Record record : records) {
//	        	// no need should be since insert already calls the nearestCentroid function
//	            int nearestCentroid = getNearestCentroid(record); 
//	            // let's do stupid way for now
//	            // assign cluster to data Page
//	            assignToCluster(nearestCentroid, record);
//	        }
//
//	        // if the assignments do not change, then the algorithm terminates
//	        boolean shouldTerminate = isLastIteration || no_changes;
//	        if (shouldTerminate) { 
//	            break; 
//	        }
//
//	        // at the end of each iteration we should relocate the centroids
//	        // Can we store our old centroids??
//	        // We need to store it to get the deleted one, if not we can just call assignTocluster
//	        // NEW PLAN:
//	        // WE get all possible centroid value
//	        // but we update the actual index later, because we need it for deleting anyway
//	        // getNewCentroids()
//	        updateCentroids();
//	    }
//
//	    return lastState;
//	}
//	
//	/**
//	 * Update all centroid
//	 * Read from old centroid -> calculate each cluster's average to get new centorid.
//	 * @return True if there is changes false if none.
//	 */
//	private boolean updateCentroids() {
//		// open the old centroid file
//		if (rfCentroid == null) {
//			String tblname = ii.indexName() + 0; // centroid listed at file indexed 0
//			TableInfo ti = new TableInfo(tblname, schema(keyType)); 
//			
//			// the underlying record file should not perform logging
//			this.rfCentroid = ti.open(tx, false);
//		}
//		// DO : I don't think it is needed
//		// initialize the file header if needed
////		if (rfCentroid.fileSize() == 0) throw new IllegalStateException("No cluster assigned yet");
//		rfCentroid.beforeFirst();
//	    	   
//		
//		// index data file start from 1
//		for (int i = 1; rfCentroid.next(); i++) {
//			boolean changes = false;
//			SearchKey curSK = getKey();
//			int curId = 0;
//			while(curSK.get(i).getType().getSqlType() != Types.ARRAY) curId++;
//			VectorConstant curCentroid = (VectorConstant) curSK.get(curId);
//			
//			VectorConstant newCentroid = findAverage(curCentroid, curId); // find the new centroid
//			
//			// check if changes
//			if (!newCentroid.equals(curCentroid)) {
//				changes = true;
//				// sus
//				RecordId curRid = getCentroidRecordId();
//				rfCentroid.delete(curRid);
//				rfCentroid.insert(curRid);
//			
//				// ? if it is 0 then OK ?
//				rfCentroid.setVal(keyFieldName(0), newCentroid);
//				rfCentroid.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(curRid.block()
//						.number()));
//				rfCentroid.setVal(SCHEMA_RID_ID, new IntegerConstant(curRid.id()));
//			}
// 		}
//		return changes;
//	}
//	
//	// TODO: Implement find centroid's new location by average every data point value
//	private VectorConstant findAverage(VectorConstant curCentroid, int centroidId) {
//		String tblname = ii.indexName() + centroidId;
//		
//		// Return directly if there is no Data File on current Centroid
//		long size = fileSize(tblname);
//		if (size == 0)
//	        return curCentroid;
//		
//		VectorConstant avgVec = VectorConstant.zeros(vecDim); 
//
//		// prepare file
//	    TableInfo ti = new TableInfo(tblname, schema(keyType));
//		this.rf = ti.open(tx, true);
//
//		rf.beforeFirst();
//		
//		int total_rec = 0;
//		while (rf.next()) {
//			SearchKey sk = getKey();
//			VectorConstant curVec = (VectorConstant) sk.get(0);
//			avgVec = (VectorConstant) avgVec.add(curVec);
//			total_rec++;
//		}
//		rf.close();
//		
//		avgVec.div(new IntegerConstant(total_rec));
//
//		return avgVec;
//	}
//	
//	public void assignToCluster(int centroidId, Record dataRecord) {
//		String tblname = ii.indexName() + centroidId;
//		TableInfo ti = new TableInfo(tblname, schema(keyType));
//		this.rf = ti.open(tx, false);
//		
//		// initialize the file header if needed
//		if (rf.fileSize() == 0)
//			RecordFile.formatFileHeader(ti.fileName(), tx);
//		
//		// get record Id
//		RecordId dataRecordId = dataRecord.getRecordId();
//		
//		// get the vector constant field again
//		for (fldName in record.fields()) {
//			if (fldName.type() == TYPES.ARRAY)
//				VectorConstant embFieldData = record.getVal(fldName);
//			else
//				throw IllegalArgumentException("There are no vector fields ");
//		}
//		
//		// WARNING!!: be careful about closing and opening here
//		// the inside of this search key should be the block we want to delete
//		currentSearchKey = new SearchKey();
////		beforeFirst(new SearchRange(key));
//		// delete old position, if wwe don't have old position then skip this
////		delete(currentSearchKey, dataRecordId, false);
//		// insert new position
//		insert(currentSearchKey, dataRecordId, false);
//		rf.close();
////		rf.beforeFirst();
//	}
//	
//
//}
