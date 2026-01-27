#include "firestore_logger.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <algorithm>
#include <cctype>

namespace duckdb {

// ============================================================================
// Log Level Utilities
// ============================================================================

const char *LogLevelToString(FirestoreLogLevel level) {
	switch (level) {
	case FirestoreLogLevel::DEBUG:
		return "DEBUG";
	case FirestoreLogLevel::INFO:
		return "INFO";
	case FirestoreLogLevel::WARN:
		return "WARN";
	case FirestoreLogLevel::ERROR:
		return "ERROR";
	case FirestoreLogLevel::NONE:
		return "NONE";
	default:
		return "UNKNOWN";
	}
}

FirestoreLogLevel ParseLogLevel(const std::string &str) {
	std::string upper = str;
	std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char c) { return std::toupper(c); });

	if (upper == "DEBUG")
		return FirestoreLogLevel::DEBUG;
	if (upper == "INFO")
		return FirestoreLogLevel::INFO;
	if (upper == "WARN" || upper == "WARNING")
		return FirestoreLogLevel::WARN;
	if (upper == "ERROR")
		return FirestoreLogLevel::ERROR;
	if (upper == "NONE" || upper == "OFF")
		return FirestoreLogLevel::NONE;

	// Default to NONE for invalid values
	return FirestoreLogLevel::NONE;
}

// ============================================================================
// FirestoreLogEntry Implementation
// ============================================================================

std::string FirestoreLogEntry::Format() const {
	std::ostringstream ss;

	// Format timestamp
	auto time_t = std::chrono::system_clock::to_time_t(timestamp);
	auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(timestamp.time_since_epoch()) % 1000;

	std::tm tm_buf;
#ifdef _WIN32
	localtime_s(&tm_buf, &time_t);
#else
	localtime_r(&time_t, &tm_buf);
#endif

	ss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
	ss << "." << std::setfill('0') << std::setw(3) << ms.count();

	// Log level
	ss << " [" << LogLevelToString(level) << "]";

	// Source location (if available)
	if (file && function) {
		// Extract just the filename from the path
		std::string filepath(file);
		auto pos = filepath.find_last_of("/\\");
		std::string filename = (pos != std::string::npos) ? filepath.substr(pos + 1) : filepath;
		ss << " " << filename << ":" << line << " " << function << "()";
	}

	// Message
	ss << " - " << message;

	return ss.str();
}

// ============================================================================
// StderrLogSink Implementation
// ============================================================================

void StderrLogSink::Log(const FirestoreLogEntry &entry) {
	std::cerr << "[FireDuckExt] " << entry.Format() << std::endl;
}

// ============================================================================
// FileLogSink Implementation
// ============================================================================

FileLogSink::FileLogSink(const std::string &filepath) {
	file_.open(filepath, std::ios::out | std::ios::app);
}

FileLogSink::~FileLogSink() {
	if (file_.is_open()) {
		file_.close();
	}
}

void FileLogSink::Log(const FirestoreLogEntry &entry) {
	if (!file_.is_open())
		return;

	std::lock_guard<std::mutex> lock(mutex_);
	file_ << entry.Format() << std::endl;
	file_.flush();
}

// ============================================================================
// FirestoreLogger Implementation
// ============================================================================

FirestoreLogger &FirestoreLogger::Instance() {
	static FirestoreLogger instance;
	return instance;
}

FirestoreLogger::FirestoreLogger() : level_(FirestoreLogLevel::NONE), sink_(std::make_shared<NullLogSink>()) {
}

void FirestoreLogger::SetLogLevel(FirestoreLogLevel level) {
	std::lock_guard<std::mutex> lock(mutex_);
	level_ = level;

	// If enabling logging and still using NullLogSink, switch to stderr
	if (level != FirestoreLogLevel::NONE && std::dynamic_pointer_cast<NullLogSink>(sink_)) {
		sink_ = std::make_shared<StderrLogSink>();
	}
}

FirestoreLogLevel FirestoreLogger::GetLogLevel() const {
	std::lock_guard<std::mutex> lock(mutex_);
	return level_;
}

void FirestoreLogger::SetSink(std::shared_ptr<FirestoreLogSink> sink) {
	std::lock_guard<std::mutex> lock(mutex_);
	sink_ = sink ? sink : std::make_shared<NullLogSink>();
}

std::shared_ptr<FirestoreLogSink> FirestoreLogger::GetSink() const {
	std::lock_guard<std::mutex> lock(mutex_);
	return sink_;
}

void FirestoreLogger::ResetToDefault() {
	std::lock_guard<std::mutex> lock(mutex_);
	level_ = FirestoreLogLevel::NONE;
	sink_ = std::make_shared<NullLogSink>();
}

void FirestoreLogger::Log(FirestoreLogLevel level, const std::string &message, const char *file, int line,
                          const char *function) {
	// Quick check without lock
	if (level < level_ || level_ == FirestoreLogLevel::NONE) {
		return;
	}

	FirestoreLogEntry entry;
	entry.level = level;
	entry.message = message;
	entry.timestamp = std::chrono::system_clock::now();
	entry.file = file;
	entry.line = line;
	entry.function = function;

	std::shared_ptr<FirestoreLogSink> sink;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		sink = sink_;
	}

	if (sink) {
		sink->Log(entry);
	}
}

} // namespace duckdb
