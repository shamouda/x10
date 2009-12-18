package x10cpp.debug;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import polyglot.util.QuotedStringTokenizer;

/**
 * Map from captured variable name to type.
 * Used for closures.
 * 
 * @author igor
 */
public class ClosureVariableMap extends StringTable {

	public static final String VARIABLE_NAME = "CVMAP";

	private HashMap<String, Integer> map = new HashMap<String, Integer>();

	/**
	 * 
	 */
	public ClosureVariableMap() {
		this(new ArrayList<String>());
	}

	private ClosureVariableMap(ArrayList<String> strings) {
		super(strings);
	}

	/**
	 * @param name the variable name
	 * @param type the type of the variable
	 */
	public void put(String name, String type) {
		map.put(name, stringId(type));
	}

	/**
	 * @param name the variable name
	 * @return the type of the variable, or null if none
	 */
	public String get(String name) {
		return lookupString(map.get(name));
	}

	/**
	 * @return the variables captured by this closure
	 */
	public String[] getVariables() {
		return map.keySet().toArray(new String[map.size()]);
	}

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (String name : map.keySet()) {
            Integer type = map.get(name);
            sb.append("  ").append(name).append(": ");
            sb.append(lookupString(type)).append("\n");
        }
        return sb.toString();
    }

    /**
     * Produces a string suitable for initializing a field in the generated file.
     */
    public String exportMap() {
        final StringBuilder sb = new StringBuilder();
        sb.append("F");
        exportStringMap(sb);
        sb.append(" V{");
        for (String name : map.keySet()) {
            Integer type = map.get(name);
            sb.append(name).append(":");
            sb.append(type).append(",");
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Parses a string into a {@link ClosureVariableMap}.
     * @param input the input string
     */
    public static ClosureVariableMap importMap(String input) {
        StringTokenizer st = new QuotedStringTokenizer(input, " ", "\"\'", '\\', true);
        String s = st.nextToken("{}");
        assert (s.equals("F"));
        ArrayList<String> strings = importStringMap(st);
        ClosureVariableMap res = new ClosureVariableMap(strings);
        if (!st.hasMoreTokens())
        	return res;
        s = st.nextToken("{}");
        assert (s.equals(" V"));
        s = st.nextToken();
        assert (s.equals("{"));
        while (st.hasMoreTokens()) {
            String t = st.nextToken(":}");
            if (t.equals("}"))
                break;
            String n = t;
            t = st.nextToken();
            assert (t.equals(":"));
            int p = Integer.parseInt(st.nextToken(","));
            res.map.put(n, p);
            t = st.nextToken();
            assert (t.equals(","));
        }
        assert (!st.hasMoreTokens());
        return res;
    }
}
