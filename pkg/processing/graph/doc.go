// Package graph provides formal graph versions for a graph of
// generated sub graph based on a metamodel.
//
// It requires wrappers (Node) for generated db objects, which provide
// sub version node sets according to their spec, which can be
// combined to a graph object, which finally provides
// formal graph versions.
// Those version must be reached after a successful processing
// of the given nodes by the engine.
package graph
