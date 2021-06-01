package main.type;

import java.util.Objects;

public class Value {

    private Integer integer;
    private String string;
    private Boolean bool;

    private TypeID typeID;

    public Value(Integer integer) {
        this.integer = integer;
        this.string = null;
        this.bool = null;
        typeID = TypeID.INTEGER_TYPE;
    }

    public Value(String string) {
        this.integer = null;
        this.string = string;
        this.bool = null;
        typeID = TypeID.STRING_TYPE;
    }

    public Value(Boolean bool) {
        this.integer = null;
        this.string = null;
        this.bool = bool;
        typeID = TypeID.BOOLEAN_TYPE;
    }

    public Value(Value o){
        this.integer = o.integer;
        this.string = o.string;
        this.bool = o.bool;
        this.typeID = o.typeID;

    }
// A value is an abstract class that represents a view over SQL data stored in
// some materialized state. All values have a type and comparison functions, but
// subclasses implement other type-specific functionality.

    // BOOLEAN and TINYINT
    public void swap(Value first, Value second) {
        switch(typeID){
            case STRING_TYPE :
                String tempString = second.string;
                second.string = first.string;
                first.string = tempString;
                break;
            case INTEGER_TYPE:
                Integer tempInt = second.integer;
                second.integer = first.integer;
                first.integer = tempInt;
                break;
            case BOOLEAN_TYPE:
                Boolean tempBool = second.bool;
                second.bool = first.bool;
                first.bool = tempBool;
                break;
        }
    }

    public Value castAs(TypeID targetTypeID) {
        Value newValue = null;
        switch (targetTypeID){
            case INTEGER_TYPE:
                Integer newInteger = new Integer(-1);
                switch(typeID){
                    case INTEGER_TYPE:
                        newInteger = new Integer(integer);
                        break;
                    case STRING_TYPE:
                        newInteger = new Integer(string);
                        break;
                    case BOOLEAN_TYPE:
                        newInteger = new Integer(bool ? 1 : 0);
                        break;
                }
                newValue =  new Value(newInteger);
                break;
            case STRING_TYPE:
                String newString = new String();
                switch(typeID){
                    case INTEGER_TYPE:
                        newString = Integer.toString(integer);
                        break;
                    case STRING_TYPE:
                        newString = new String(string);
                        break;
                    case BOOLEAN_TYPE:
                        newString = String.valueOf(bool);
                        break;
                }
                newValue =  new Value(newString);
                break;
            case BOOLEAN_TYPE:
                Boolean newBoolean = new Boolean(false);
                switch(typeID){
                    case INTEGER_TYPE:
                        newBoolean = integer > 0 ? true: false;
                        break;
                    case STRING_TYPE:
                        newBoolean = string.isEmpty() ? false: true;
                        break;
                    case BOOLEAN_TYPE:
                        newBoolean = new Boolean(bool);
                        break;
                }
                newValue =  new Value(newBoolean);
                break;
        }
        return newValue;

    }
    // Comparison Methods

    public boolean compareEquals(Value o) {
        switch (typeID){
            case INTEGER_TYPE:
                return this.integer.equals(o.integer);
            case STRING_TYPE:
                return this.string.equals(o.string);
            case BOOLEAN_TYPE:
                return this.bool.equals(o.bool);
        }
        return false;
    }

    public boolean compareNotEquals(Value o) {
        switch (typeID){
            case INTEGER_TYPE:
                return !this.integer.equals(o.integer);
            case STRING_TYPE:
                return !this.string.equals(o.string);
            case BOOLEAN_TYPE:
                return !this.bool.equals(o.bool);
        }
        return false;
    }

    public boolean compareLessThan(Value o) {
        switch (typeID){
            case INTEGER_TYPE:
                return this.integer.compareTo(o.integer) < 0;
            case STRING_TYPE:
                return this.string.compareTo(o.string) < 0;
            case BOOLEAN_TYPE:
                return this.bool.compareTo(o.bool) < 0;
        }
        return false;
    }

    public boolean compareLessThanEquals(Value o) {
        switch (typeID){
            case INTEGER_TYPE:
                return this.integer.compareTo(o.integer) <= 0;
            case STRING_TYPE:
                return this.string.compareTo(o.string) <= 0;
            case BOOLEAN_TYPE:
                return this.bool.compareTo(o.bool) <= 0;
        }
        return false;
    }

    public boolean compareGreaterThan(Value o) {
        switch (typeID){
            case INTEGER_TYPE:
                return this.integer.compareTo(o.integer) > 0;
            case STRING_TYPE:
                return this.string.compareTo(o.string) > 0;
            case BOOLEAN_TYPE:
                return this.bool.compareTo(o.bool) > 0;
        }
        return false;

    }

    public boolean compareGreaterThanEquals(Value o) {
        switch (typeID){
            case INTEGER_TYPE:
                return this.integer.compareTo(o.integer) >= 0;
            case STRING_TYPE:
                return this.string.compareTo(o.string) >= 0;
            case BOOLEAN_TYPE:
                return this.bool.compareTo(o.bool) >= 0;
        }
        return false;
    }

    // Other mathematical functions
    public Value add(Value o){
        Value newValue = null;
        switch (typeID){
            case INTEGER_TYPE:
                return new Value(this.integer + o.integer);
            case STRING_TYPE:
                return new Value(this.string + o.string);
            default:
                break;
        }
        return newValue;
    }

    public Value subtract(Value o){
        if(typeID == TypeID.INTEGER_TYPE)
            return new Value(integer - o.integer);
        return null;
    }
    public Value multiply(Value o) {
        if(typeID == TypeID.INTEGER_TYPE)
            return new Value(integer * o.integer);
        return null;
    }
    public Value divide(Value o)  {
        try{
            if(o.integer == 0)
                throw new Exception("Division by zero encountered");
            if(typeID == TypeID.INTEGER_TYPE)
                return new Value(integer / o.integer);
        }
        catch(Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return null;

    }

    public Value modulo(Value o) {
        if(typeID == TypeID.INTEGER_TYPE)
            return new Value(integer % o.integer);
        return null;
    }

    public Value min(Value o){
        switch (typeID){
            case INTEGER_TYPE:
                return new Value(this.compareLessThan(o) ? this.integer : o.integer);
            case STRING_TYPE:
                return new Value(this.compareLessThan(o) ? this.string : o.string);
        }
        return null;
    }

    public Value max(Value o){
        switch (typeID){
            case INTEGER_TYPE:
                return new Value(this.compareGreaterThan(o) ? this.integer : o.integer);
            case STRING_TYPE:
                return new Value(this.compareGreaterThan(o) ? this.string : o.string);
        }
        return null;

    }

    public Value sqrt() {
        if(typeID == TypeID.INTEGER_TYPE)
            return new Value((int) Math.sqrt(integer));
        return null;
    }


    @Override
    public boolean equals(Object o) {
        Value v = (Value)o;
        if (this == v || v == null || typeID != v.typeID) return false;
        switch (typeID){
            case INTEGER_TYPE :
                return integer.equals(v.integer);
            case BOOLEAN_TYPE:
                return bool.equals(v.bool);
            case STRING_TYPE:
                return string.equals(v.string);
        }
        return false;
    }


    public boolean isZero() {
        switch (typeID){
            case INTEGER_TYPE:
                return integer == 0;
            case STRING_TYPE:
                return string.isEmpty();
        }
        return false;

    }
    public boolean isNull() {
        return integer == null || string == null || bool == null;
    }

    // Serialize this value into the given storage space. The inlined parameter
    // indicates whether we are allowed to inline this value into the storage
    // space, or whether we must store only a reference to this value. If inlined
    // is false, we may use the provided data pool to allocate space for this
    // value, storing a reference into the allocated pool space in the storage.

    public TypeID getTypeID() {
        return typeID;
    }


    // Return a string version of this value
    public String toString() {
        switch (typeID){
            case INTEGER_TYPE:
                return integer.toString();
            case STRING_TYPE:
                return new String(string);
            case BOOLEAN_TYPE:
                return bool.toString();
            default:
                return "";
        }
    }

    // Create a copy of this value
    public Value copy() {
        return new Value(this);
    }

    public Boolean getAsBoolean(){

        switch (typeID){
            case BOOLEAN_TYPE : return  bool;
            case INTEGER_TYPE: return integer != 0;
            case STRING_TYPE: return !string.isEmpty();
        }
        return false;
    }

    public Integer getAsInteger(){
        return integer;
    }

    public String getAsString(){
        return string;
    }

    @Override
    public int hashCode() {
        return Objects.hash(integer, string, bool, typeID);
    }
}
