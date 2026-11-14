#include "test_harness.hpp"
#include "firestore_path_utils.hpp"

#include <string>

using duckdb::FirestoreCollectionGroupNameError;
using duckdb::IsFirestoreCollectionGroup;
using duckdb::ResolveFirestoreCollectionGroupDocumentPath;

namespace {

// The resolved path, or "ERROR: <message>" when the path is refused, so a
// single assertion shows which way a case went.
std::string Resolve(const std::string &group, const std::string &path) {
	std::string resolved = "left unchanged on error";
	auto error = ResolveFirestoreCollectionGroupDocumentPath(group, path, resolved);
	if (!error.empty()) {
		FD_REQUIRE_EQ(resolved, std::string());
		return "ERROR: " + error;
	}
	return resolved;
}

bool Contains(const std::string &haystack, const std::string &needle) {
	return haystack.find(needle) != std::string::npos;
}

} // namespace

// ---------------------------------------------------------------- segments

FD_TEST("path: segments ignore repeated, leading and trailing slashes") {
	FD_REQUIRE_EQ(duckdb::CountFirestorePathSegments(""), 0);
	FD_REQUIRE_EQ(duckdb::CountFirestorePathSegments("users"), 1);
	FD_REQUIRE_EQ(duckdb::CountFirestorePathSegments("users/u1"), 2);
	FD_REQUIRE_EQ(duckdb::CountFirestorePathSegments("/users//u1/"), 2);
}

FD_TEST("path: a document path has an even, non-zero number of segments") {
	FD_REQUIRE(duckdb::IsFirestoreDocumentPath("users/u1"));
	FD_REQUIRE(duckdb::IsFirestoreDocumentPath("users/u1/orders/o1"));
	FD_REQUIRE_FALSE(duckdb::IsFirestoreDocumentPath(""));
	FD_REQUIRE_FALSE(duckdb::IsFirestoreDocumentPath("users"));
	FD_REQUIRE_FALSE(duckdb::IsFirestoreDocumentPath("users/u1/orders"));
}

FD_TEST("path: a collection group is never a document-path scan") {
	FD_REQUIRE(duckdb::IsFirestoreDocumentPathCollection("users/u1"));
	FD_REQUIRE_FALSE(duckdb::IsFirestoreDocumentPathCollection("~users/u1"));
	FD_REQUIRE_FALSE(duckdb::IsFirestoreDocumentPathCollection("users"));
}

FD_TEST("path: split puts a nested collection's parent in the URL") {
	std::string parent, id;
	duckdb::SplitFirestoreCollectionPath("users/u1/orders", false, parent, id);
	FD_REQUIRE_EQ(parent, std::string("users/u1"));
	FD_REQUIRE_EQ(id, std::string("orders"));

	duckdb::SplitFirestoreCollectionPath("users", false, parent, id);
	FD_REQUIRE_EQ(parent, std::string());
	FD_REQUIRE_EQ(id, std::string("users"));

	duckdb::SplitFirestoreCollectionPath("~orders", true, parent, id);
	FD_REQUIRE_EQ(parent, std::string());
	FD_REQUIRE_EQ(id, std::string("orders"));

	// Flagged as a group without the marker: the id is used as given.
	duckdb::SplitFirestoreCollectionPath("orders", true, parent, id);
	FD_REQUIRE_EQ(parent, std::string());
	FD_REQUIRE_EQ(id, std::string("orders"));
}

// ---------------------------------------------------------------- groups

FD_TEST("path: only a leading '~' marks a collection group") {
	FD_REQUIRE(IsFirestoreCollectionGroup("~orders"));
	FD_REQUIRE(IsFirestoreCollectionGroup("~"));
	FD_REQUIRE_FALSE(IsFirestoreCollectionGroup(""));
	FD_REQUIRE_FALSE(IsFirestoreCollectionGroup("orders"));
	FD_REQUIRE_FALSE(IsFirestoreCollectionGroup("users/u1/~orders"));
}

FD_TEST("path: a collection group name is one collection id") {
	FD_REQUIRE_EQ(FirestoreCollectionGroupNameError("~orders"), std::string());

	auto empty = FirestoreCollectionGroupNameError("~");
	FD_REQUIRE(Contains(empty, "names no collection"));

	auto nested = FirestoreCollectionGroupNameError("~users/u1/orders");
	FD_REQUIRE(Contains(nested, "is a path"));
	FD_REQUIRE(Contains(nested, "such as '~orders'"));

	// A trailing slash leaves nothing to suggest from the name itself.
	auto trailing = FirestoreCollectionGroupNameError("~orders/");
	FD_REQUIRE(Contains(trailing, "is a path"));
	FD_REQUIRE(Contains(trailing, "such as '~orders'"));
}

// ---------------------------------------------------------------- resolve

FD_TEST("path: a group write takes the document's full path as given") {
	FD_REQUIRE_EQ(Resolve("~orders", "users/u1/orders/o1"), std::string("users/u1/orders/o1"));
	// A root-level collection with the group's id is part of the group too.
	FD_REQUIRE_EQ(Resolve("~orders", "orders/o1"), std::string("orders/o1"));
	// Deeper nesting is fine as long as the document sits in the group.
	FD_REQUIRE_EQ(Resolve("~orders", "a/1/b/2/orders/o1"), std::string("a/1/b/2/orders/o1"));
}

FD_TEST("path: one leading slash is dropped") {
	FD_REQUIRE_EQ(Resolve("~orders", "/users/u1/orders/o1"), std::string("users/u1/orders/o1"));
}

FD_TEST("path: a bare document id is refused with the full-path form") {
	auto error = Resolve("~orders", "o1");
	FD_REQUIRE(Contains(error, "ERROR: 'o1' is not a full document path"));
	FD_REQUIRE(Contains(error, "'<parent path>/orders/<document id>'"));
	FD_REQUIRE(Contains(error, "firestore_scan('~orders')"));

	// Any odd number of segments names a collection, not a document.
	FD_REQUIRE(Contains(Resolve("~orders", "users/u1/orders"), "is not a full document path"));
}

FD_TEST("path: a document in another collection is refused") {
	auto top_level = Resolve("~orders", "users/u1");
	FD_REQUIRE_EQ(
	    top_level,
	    std::string("ERROR: document 'users/u1' is in collection 'users', not in collection group '~orders'"));

	auto sibling = Resolve("~orders", "users/u1/profile/main");
	FD_REQUIRE(Contains(sibling, "is in collection 'profile'"));

	// Collection ids are case-sensitive in Firestore.
	FD_REQUIRE(Contains(Resolve("~orders", "users/u1/Orders/o1"), "is in collection 'Orders'"));
}

FD_TEST("path: empty paths and empty segments are refused") {
	FD_REQUIRE(Contains(Resolve("~orders", ""), "document path is empty"));
	FD_REQUIRE(Contains(Resolve("~orders", "/"), "document path is empty"));
	FD_REQUIRE(Contains(Resolve("~orders", "users//orders/o1"), "has an empty segment"));
	FD_REQUIRE(Contains(Resolve("~orders", "users/u1/orders/o1/"), "has an empty segment"));
	FD_REQUIRE(Contains(Resolve("~orders", "//users/u1/orders/o1"), "has an empty segment"));
}

FD_TEST("path: an unusable group name is refused before the path is read") {
	FD_REQUIRE(Contains(Resolve("~", "users/u1/orders/o1"), "names no collection"));
	FD_REQUIRE(Contains(Resolve("~users/u1/orders", "users/u1/orders/o1"), "is a path"));
}

FD_TEST("path: a group id given without the marker resolves the same way") {
	FD_REQUIRE_EQ(Resolve("orders", "users/u1/orders/o1"), std::string("users/u1/orders/o1"));
	FD_REQUIRE(Contains(Resolve("orders", "users/u1"), "is in collection 'users'"));
}
