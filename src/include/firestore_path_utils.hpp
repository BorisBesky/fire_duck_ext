#pragma once

#include <string>

namespace duckdb {

// Count the number of slash-delimited path segments, ignoring repeated,
// leading, and trailing separators.
int CountFirestorePathSegments(const std::string &path);

// Firestore document paths have an even number of segments, such as
// "users/uid" or "users/uid/orders/order_id".
bool IsFirestoreDocumentPath(const std::string &path);

// Collection-group scans use a leading '~' prefix, so they are never treated
// as document-path scans even if the remaining text happens to have even segments.
bool IsFirestoreDocumentPathCollection(const std::string &collection);

// Split a collection spec into the parent document path (which goes in the
// runQuery URL) and the final collection id (which goes in
// structuredQuery.from.collectionId). For nested subcollections such as
// "users/uid/orders", parent_path="users/uid" and collection_id="orders".
// For top-level collections and collection groups (leading '~'), parent_path is
// empty; the '~' marker is stripped from collection_id.
void SplitFirestoreCollectionPath(const std::string &collection, bool is_collection_group, std::string &parent_path,
                                  std::string &collection_id);

// Whether a collection spec names a collection group: a leading '~', as in
// "~orders" (every collection called "orders", wherever it sits).
bool IsFirestoreCollectionGroup(const std::string &collection);

// Why a collection-group spec cannot be written to, or an empty string when it
// can. The name after '~' must be a single collection id: "~" names nothing,
// and "~users/u1/orders" is a path, not a group.
std::string FirestoreCollectionGroupNameError(const std::string &collection_group);

// Resolve the document a collection-group write ("~orders") targets.
//
// A collection group has no single parent, so a write must name each document
// by its full path ("users/u1/orders/o1") -- the __document_id that
// firestore_scan('~orders') returns. One leading '/' is accepted and dropped.
//
// Returns an empty string and sets `resolved_path` when the path is usable.
// Otherwise returns why not and leaves `resolved_path` empty. A path is refused
// when the group name is unusable (see FirestoreCollectionGroupNameError), when
// it is not a document path (an odd number of segments, such as a bare id
// "o1"), when a segment is empty ("users//orders/o1"), or when its document
// lies in another collection ("users/u1" is not in group "~orders").
std::string ResolveFirestoreCollectionGroupDocumentPath(const std::string &collection_group,
                                                        const std::string &document_path, std::string &resolved_path);

} // namespace duckdb
