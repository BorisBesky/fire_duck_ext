#pragma once

#include <string>
#include <memory>
#include <functional>
#include <chrono>
#include <mutex>
#include <fstream>

namespace duckdb {

// ============================================================================
// Log Levels
// ============================================================================

enum class FirestoreLogLevel {
	DEBUG = 0, // Detailed diagnostic information
	INFO = 1,  // General operational information
	WARN = 2,  // Warning conditions
	ERR = 3,   // Error conditions
	NONE = 4   // Disable all logging
};

// Convert log level to string
const char *LogLevelToString(FirestoreLogLevel level);

// Parse log level from string (case insensitive)
FirestoreLogLevel ParseLogLevel(const std::string &str);

// ============================================================================
// Log Entry
// ============================================================================

struct FirestoreLogEntry {
	FirestoreLogLevel level;
	std::string message;
	std::chrono::system_clock::time_point timestamp;
	const char *file;
	int line;
	const char *function;

	// Format entry as string
	std::string Format() const;
};

// ============================================================================
// Log Sink Interface
// ============================================================================

class FirestoreLogSink {
public:
	virtual ~FirestoreLogSink() = default;
	virtual void Log(const FirestoreLogEntry &entry) = 0;
};

// Default sink that does nothing (for production)
class NullLogSink : public FirestoreLogSink {
public:
	void Log(const FirestoreLogEntry &entry) override {
	}
};

// Sink that writes to stderr
class StderrLogSink : public FirestoreLogSink {
public:
	void Log(const FirestoreLogEntry &entry) override;
};

// Sink that writes to a file
class FileLogSink : public FirestoreLogSink {
public:
	explicit FileLogSink(const std::string &filepath);
	~FileLogSink();
	void Log(const FirestoreLogEntry &entry) override;
	bool IsOpen() const {
		return file_.is_open();
	}

private:
	std::ofstream file_;
	std::mutex mutex_;
};

// Sink that calls a user-provided callback
class CallbackLogSink : public FirestoreLogSink {
public:
	using Callback = std::function<void(const FirestoreLogEntry &)>;
	explicit CallbackLogSink(Callback cb) : callback_(std::move(cb)) {
	}
	void Log(const FirestoreLogEntry &entry) override {
		if (callback_)
			callback_(entry);
	}

private:
	Callback callback_;
};

// ============================================================================
// Global Logger
// ============================================================================

class FirestoreLogger {
public:
	// Get singleton instance
	static FirestoreLogger &Instance();

	// Configuration
	void SetLogLevel(FirestoreLogLevel level);
	FirestoreLogLevel GetLogLevel() const;

	void SetSink(std::shared_ptr<FirestoreLogSink> sink);
	std::shared_ptr<FirestoreLogSink> GetSink() const;
	void ResetToDefault(); // Resets to NullLogSink

	// Check if a level would be logged
	bool ShouldLog(FirestoreLogLevel level) const {
		return level >= level_ && level_ != FirestoreLogLevel::NONE;
	}

	// Core logging method
	void Log(FirestoreLogLevel level, const std::string &message, const char *file = nullptr, int line = 0,
	         const char *function = nullptr);

	// Convenience methods
	void Debug(const std::string &msg, const char *file = nullptr, int line = 0, const char *func = nullptr) {
		Log(FirestoreLogLevel::DEBUG, msg, file, line, func);
	}

	void Info(const std::string &msg, const char *file = nullptr, int line = 0, const char *func = nullptr) {
		Log(FirestoreLogLevel::INFO, msg, file, line, func);
	}

	void Warn(const std::string &msg, const char *file = nullptr, int line = 0, const char *func = nullptr) {
		Log(FirestoreLogLevel::WARN, msg, file, line, func);
	}

	void Error(const std::string &msg, const char *file = nullptr, int line = 0, const char *func = nullptr) {
		Log(FirestoreLogLevel::ERR, msg, file, line, func);
	}

	// Delete copy/move constructors
	FirestoreLogger(const FirestoreLogger &) = delete;
	FirestoreLogger &operator=(const FirestoreLogger &) = delete;

private:
	FirestoreLogger();

	FirestoreLogLevel level_;
	std::shared_ptr<FirestoreLogSink> sink_;
	mutable std::mutex mutex_;
};

// ============================================================================
// Logging Macros
// ============================================================================

// These macros automatically capture source location

#define FS_LOG_DEBUG(msg)                                                                                              \
	do {                                                                                                               \
		if (::duckdb::FirestoreLogger::Instance().ShouldLog(::duckdb::FirestoreLogLevel::DEBUG)) {                     \
			::duckdb::FirestoreLogger::Instance().Debug(msg, __FILE__, __LINE__, __func__);                            \
		}                                                                                                              \
	} while (0)

#define FS_LOG_INFO(msg)                                                                                               \
	do {                                                                                                               \
		if (::duckdb::FirestoreLogger::Instance().ShouldLog(::duckdb::FirestoreLogLevel::INFO)) {                      \
			::duckdb::FirestoreLogger::Instance().Info(msg, __FILE__, __LINE__, __func__);                             \
		}                                                                                                              \
	} while (0)

#define FS_LOG_WARN(msg)                                                                                               \
	do {                                                                                                               \
		if (::duckdb::FirestoreLogger::Instance().ShouldLog(::duckdb::FirestoreLogLevel::WARN)) {                      \
			::duckdb::FirestoreLogger::Instance().Warn(msg, __FILE__, __LINE__, __func__);                             \
		}                                                                                                              \
	} while (0)

#define FS_LOG_ERROR(msg)                                                                                              \
	do {                                                                                                               \
		if (::duckdb::FirestoreLogger::Instance().ShouldLog(::duckdb::FirestoreLogLevel::ERR)) {                       \
			::duckdb::FirestoreLogger::Instance().Error(msg, __FILE__, __LINE__, __func__);                            \
		}                                                                                                              \
	} while (0)

// Conditional logging with format-like syntax
#define FS_LOG_DEBUG_IF(cond, msg)                                                                                     \
	do {                                                                                                               \
		if (cond) {                                                                                                    \
			FS_LOG_DEBUG(msg);                                                                                         \
		}                                                                                                              \
	} while (0)
#define FS_LOG_INFO_IF(cond, msg)                                                                                      \
	do {                                                                                                               \
		if (cond) {                                                                                                    \
			FS_LOG_INFO(msg);                                                                                          \
		}                                                                                                              \
	} while (0)
#define FS_LOG_WARN_IF(cond, msg)                                                                                      \
	do {                                                                                                               \
		if (cond) {                                                                                                    \
			FS_LOG_WARN(msg);                                                                                          \
		}                                                                                                              \
	} while (0)
#define FS_LOG_ERROR_IF(cond, msg)                                                                                     \
	do {                                                                                                               \
		if (cond) {                                                                                                    \
			FS_LOG_ERROR(msg);                                                                                         \
		}                                                                                                              \
	} while (0)

} // namespace duckdb
