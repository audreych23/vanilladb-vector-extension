package org.vanilladb.core.sql;

import static java.sql.Types.BIGINT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

import java.io.Serializable;

import org.vanilladb.core.util.ByteHelper;

import java.util.*;

/**
 * Vector constant stores multiple int32 values as a constant
 * This would enable vector processing in VanillaCore
 */
public class VectorConstant extends Constant implements Serializable {

    static Random random = new Random();
    private int[] vec;
    private Type type;

    public static void seed(long seed) {
        random = new Random(seed);
    }

    public static VectorConstant zeros(int dimension) {
        int[] vec = new int[dimension];
        for (int i = 0; i < dimension; i++) {
            vec[i] = 0;
        }
        return new VectorConstant(vec);
    }

    /**
     * Return a vector constant with random values
     * @param length size of the vector
     */
    public VectorConstant(int length) {
        type = new VectorType(length);

        vec = new int[length];
        for (int i = 0; i < length; i++) {
            vec[i] = random.nextInt(9999);
        }
    }

    public VectorConstant(int[] vector) {
        type = new VectorType(vector.length);
        vec = new int[vector.length];
        
        for (int i = 0; i < vector.length; i++) {
            vec[i] = vector[i];
        }
    }

    public VectorConstant(List<Integer> vector) {
        int length = vector.size();
        
        type = new VectorType(length);
        vec = new int[length];
        
        for (int i = 0; i < length; i++) {
            vec[i] = vector.get(i);
        }
    }

    /**
     * Reconstruct a vector constant from bytes
     * @param bytes bytes to reconstruct
     */
    public VectorConstant(byte[] bytes) {
        int length = bytes.length / Integer.BYTES;
        type = new VectorType(length);
        // vec = new ArrayList<>(length);
        vec = new int[length];
        for (int i = 0; i < length; i++) {
            byte[] intAsBytes = new byte[Integer.BYTES];
            int offset = i * Integer.BYTES;
            System.arraycopy(bytes, offset, intAsBytes, 0, Integer.BYTES);
            vec[i] = ByteHelper.toInteger(intAsBytes);
        }
    }

    /**
     * Return the type of the constant
     */
    @Override
    public Type getType() {
        return type;
    }

    /**
     * Return the value of the constant
     */
    @Override
    public int[] asJavaVal() {
        return vec;
    }

    /**
     * Return a copy of the vector
     * @return
     */
    public int[] copy() {
        return Arrays.copyOf(vec, vec.length);
    }


    /** 
     * Return the vector as bytes
    */
    @Override
    public byte[] asBytes() {
        int bufferSize = this.size();
        byte[] buf = new byte[bufferSize];

        for (int i = 0; i < vec.length; i++) {
            byte[] intAsBytes = ByteHelper.toBytes(vec[i]);
            int offset = i * Integer.BYTES;
            System.arraycopy(intAsBytes, 0, buf, offset, Integer.BYTES);
        }
        return buf;
    }

    /**
     * Return the size of the vector in bytes
     */
    @Override
    public int size() {
        return Integer.BYTES * vec.length;
    }

    /**
     * Return the size of the vector
     * @return size of the vector
     */
    public int dimension() {
        return vec.length;
    }

    @Override
    public Constant castTo(Type type) {
        if (getType().equals(type))
            return this;
        switch (type.getSqlType()) {
            case VARCHAR:
                return new VarcharConstant(toString(), type);
            }
        throw new IllegalArgumentException("Cannot cast vector to " + type);
    }

    public int get(int idx) {
        return vec[idx];
    }

    @Override
    public Constant add(Constant c) {
    	if (c.getType() != this.getType()) 
    		throw new IllegalArgumentException("Cannot do addition " +  this.getType().toString() + " with " + c.getType().toString());
        
    	if (c.size() != this.size())
            throw new IllegalArgumentException("Cannot do addition, vector argument should have " + dimension() + "dimension");
        
    	int[] total_vec = new int[dimension()]; 	
    	int[] vecc = (int[]) c.asJavaVal();
    	for (int i = 0; i < dimension(); i++) {
    		total_vec[i] = this.vec[i] + vecc[i];
        }
    	
        return new VectorConstant(total_vec);
    }

    @Override
    public Constant sub(Constant c) {
    	if (c.getType() != this.getType()) 
    		throw new IllegalArgumentException("Cannot do addition " +  this.getType().toString() + " with " + c.getType().toString());
        
    	if (c.size() != this.size())
            throw new IllegalArgumentException("Cannot do addition, vector argument should have " + dimension() + "dimension");
        
    	int[] total_vec = new int[dimension()]; 	
    	int[] vecc = (int[]) c.asJavaVal();
    	for (int i = 0; i < dimension(); i++) {
    		total_vec[i] = this.vec[i] - vecc[i];
        }
    	
        return new VectorConstant(total_vec);
    }

    @Override
    public Constant mul(Constant c) {
    	if (c instanceof IntegerConstant) {
    		int[] total_vec = new int[dimension()]; 	
        	for (int i = 0; i < dimension(); i++) {
        		total_vec[i] =  this.vec[i] * (Integer) c.asJavaVal();
            }
    		return new VectorConstant(total_vec);    	
    	}else if (c instanceof BigIntConstant) {
    		int[] total_vec = new int[dimension()]; 	
        	for (int i = 0; i < dimension(); i++) {
        		total_vec[i] =  (int) (this.vec[i] * (Long) c.asJavaVal());
            }
    		return new VectorConstant(total_vec);
    	}else if (c instanceof DoubleConstant) {
    		int[] total_vec = new int[dimension()]; 	
        	for (int i = 0; i < dimension(); i++) {
        		total_vec[i] =  (int) (this.vec[i] * (Double) c.asJavaVal());
            }
    		return new VectorConstant(total_vec);
		} else
			throw new IllegalArgumentException();
    }

    @Override
    public Constant div(Constant c) {
    	if (c instanceof IntegerConstant) {
    		int[] total_vec = new int[dimension()]; 	
        	for (int i = 0; i < dimension(); i++) {
        		total_vec[i] =  this.vec[i] / (Integer) c.asJavaVal();
            }
    		return new VectorConstant(total_vec);    	
    	}else if (c instanceof BigIntConstant) {
    		int[] total_vec = new int[dimension()]; 	
        	for (int i = 0; i < dimension(); i++) {
        		total_vec[i] =  (int) (this.vec[i] / (Long) c.asJavaVal());
            }
    		return new VectorConstant(total_vec);
    	}else if (c instanceof DoubleConstant) {
    		int[] total_vec = new int[dimension()]; 	
        	for (int i = 0; i < dimension(); i++) {
        		total_vec[i] =  (int) (this.vec[i] / (Double) c.asJavaVal());
            }
    		return new VectorConstant(total_vec);
		} else
			throw new IllegalArgumentException();
    }

    @Override
    public int compareTo(Constant c) {
    	if (!(c instanceof VectorConstant)) {
    		throw new IllegalArgumentException();
    	} else {
    		int[] x = (int[]) c.asJavaVal(); // cast to array from VectorConst
    		for (int i = 0; i < x.length; ++i) {
    			Integer d = (Integer) vec[i]; // cast to Integer from int
    			if (d.compareTo((Integer) x[i]) == 0) {
    				continue;
    			} else {
    				return d.compareTo((Integer) x[i]);
    			}
    		}
    		// both vectors are the same
    		return 0;
    	}
    }

    public boolean equals(VectorConstant o) {
        if (o.size() != this.size())
            return false;

        for (int i = 0; i < dimension(); i++) {
            if (vec[i] != o.get(i))
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return Arrays.toString(vec);
    }

	public Constant minValue() {
    	int dimension = dimension();
    	int[] vec = new int[dimension];
        for (int i = 0; i < dimension; i++) {
            vec[i] = Integer.MIN_VALUE;
        }
        return new VectorConstant(vec);
    }
	
    public int[] hashCode(int bands, int buckets) {
        assert dimension() % bands == 0;

        int chunkSize = dimension() / bands;

        int[] hashCodes = new int[bands];
        for (int i = 0; i < bands; i++) {
            int hashCode = (Arrays.hashCode(Arrays.copyOfRange(vec, i * chunkSize, (i + 1) * chunkSize))) % buckets;
            if (hashCode < 0)
                hashCode += buckets;
            hashCodes[i] = hashCode;
        }
        return hashCodes;
    }
}
