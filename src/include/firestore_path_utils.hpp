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

} // namespace duckdb
