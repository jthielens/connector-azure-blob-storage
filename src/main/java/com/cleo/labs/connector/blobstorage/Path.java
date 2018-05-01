package com.cleo.labs.connector.blobstorage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

public class Path {

    static public final String DEFAULT_DELIMITER = "/";
    
    /**
     * The list of String node names
     */
    private List<String> nodes;

    private String delimiter = DEFAULT_DELIMITER;

    /**
     * Internal use only: makes a new Path without attempting
     * to parse delimiters from the supplied node list.
     * @param nodes
     * @param delimiter
     */
    private Path(List<String> nodes, String delimiter) {
        this.nodes = nodes;
        this.delimiter = delimiter;
    }

    /**
     * Constructs a new empty path with the default delimiter.
     */
    public Path() {
        this(DEFAULT_DELIMITER);
    }

    /**
     * Constructs a new empty path with the specified delimiter.
     * @param delimiter the delimiter
     */
    public Path(String delimiter) {
        this.nodes = Collections.emptyList();
        this.delimiter = delimiter;
    }

    /**
     * Parses zero or more strings to
     * form a path, splitting each string by {@link #DEFAULT_DELIMITER}.
     * All null or empty strings, in the {@code parse} list or
     * as a result of splitting, are discarded.  This means that
     * leading, trailing, or multiple consecutive delimiters are
     * ignored.  An empty path has length zero.
     * @param parse a (possibly {@code null}) list of (possibly {@code null}) {@code String}s to parse
     */
    public Path parse(String...parse) {
        this.nodes = new ArrayList<>();
        if (parse != null) {
            for (String node : parse) {
                if (!Strings.isNullOrEmpty(node)) {
                    for (String element : node.split(delimiter)) {
                        if (!element.isEmpty()) {
                            nodes.add(element);
                        }
                    }
                }
            }
        }
        return this;
    }

    /**
     * Returns the number of nodes in the path, ranging
     * from {@code 0} for an empty path up.
     * @return
     */
    public int size() {
        return nodes.size();
    }

    /**
     * Returns {@code true} if the path is empty,
     * i.e. the path has length {@code 0}.
     * @return
     */
    public boolean empty() {
        return nodes.isEmpty();
    }

    /**
     * Returns the parent path, i.e. a path with the last
     * node of the path removed.  The parent of an empty path
     * is the same empty path.
     * @return a new Path shortened by 1, or {@code this} if the path is already empty
     */
    public Path parent() {
        if (nodes.isEmpty()) {
            return this;
        } else {
            return new Path(nodes.subList(0, nodes.size()-1), delimiter);
        }
    }

    /**
     * Returns a new path with one additional node added at the
     * end.  This method does not parse the supplied node for embedded
     * delimiters or check for null or empty nodes.
     * <p/>
     * To safely parse node names for addition to a path, use
     * <pre>
     *     child(new Path(nodes...))
     * </pre>
     * @param node the node name (should not be null, empty, or contain the delimiter, but these are not checked)
     * @return a longer path
     */
    public Path child(String node) {
        List<String> child = new ArrayList<>(nodes);
        child.add(node);
        return new Path(child, delimiter);
    }

    /**
     * Returns a new Path with zero or more additional nodes added at the
     * end, copied from the supplied Path.
     * @param path the Path to append to the current Path
     * @return the new Path
     */
    public Path child(Path path) {
        List<String> child = new ArrayList<>(nodes);
        child.addAll(path.nodes);
        return new Path(child, delimiter);
    }

    /**
     * Returns a new Path based on the current path but
     * shortened by {@code count} nodes from the left.  If {@code count}
     * is negative, nodes are retained from the right.  If there
     * are not that many nodes in the Path, an empty path is returned.
     * @param count the number of nodes to remove from the left or retain on the right
     * @return a shortened new Path
     */
    public Path chroot(int count) {
        return slice(count, null);
    }

    /**
     * Returns a slice of the current Path as a new Path based on a
     * starting from index (inclusive) and an ending to index (exclusive).
     * The from and to indices may be {@code null}, indicating that a reasonable
     * default (from 0 or to {@code size()}) should be used.  A negative
     * index means to back up from the end.  Invalid indices are adjusted to
     * the valid range.
     * @param fromNullable the from index (inclusive), or {@code null} to mean 0
     * @param toNullable the to index (exclusize), or {@code null} to mean {@code size()}
     * @return a (possibly empty) slice of the Path
     */
    public Path slice(Integer fromNullable, Integer toNullable) {
        int from = fromNullable==null ? 0 : fromNullable;
        int to = toNullable==null ? nodes.size() : toNullable;
        if (from < 0) {
            from = Math.max(0, nodes.size()+from);
        }
        from = Math.min(from, nodes.size());
        if (to < 0) {
            to = Math.max(0, nodes.size()+to);
        }
        to = Math.min(to, nodes.size());
        to = Math.max(from, to);
        return new Path(nodes.subList(from, to), delimiter);
    }

    /**
     * Returns the nth (0-relative) node in the Path,
     * or "" if there is no such node.  If n is negative,
     * counts from the end of the path.
     * @param n the node to return
     * @return the node
     */
    public String node(int n) {
        if (n < 0) {
            n = nodes.size()+n;
        }
        if (n < 0 || n >= nodes.size()) {
            return "";
        } else {
            return nodes.get(n);
        }
    }

    /**
     * Returns the "name" of the Path, i.e. the last node
     * or "" if the path is empty (has no nodes).
     * @return the "name" of the Path
     */
    public String name() {
        return node(-1);
    }

    /**
     * Returns all of the node names joined back together
     * with {@link #DEFAULT_DELIMITER} as a separator.  The returned
     * String neither begins nor ends with the delimiter.  ""
     * is returned for an empty Path.
     */
    @Override
    public String toString() {
        return Joiner.on(delimiter).join(nodes);
    }
}
