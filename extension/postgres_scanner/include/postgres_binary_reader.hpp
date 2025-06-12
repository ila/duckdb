//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_binary_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/interval.hpp"
#include "postgres_conversion.hpp"

namespace duckdb {

struct PostgresBinaryReader {
	explicit PostgresBinaryReader(PostgresConnection &con_p) : con(con_p) {
	}
	~PostgresBinaryReader() {
		Reset();
	}

	bool Next() {
		Reset();
		char *out_buffer;
		int len = PQgetCopyData(con.GetConn(), &out_buffer, 0);
		auto new_buffer = data_ptr_cast(out_buffer);

		// len -1 signals end
		if (len == -1) {
			return false;
		}

		// len -2 is error
		// we expect at least 2 bytes in each message for the tuple count
		if (!new_buffer || len < sizeof(int16_t)) {
			throw IOException("Unable to read binary COPY data from Postgres: %s",
			                  string(PQerrorMessage(con.GetConn())));
		}
		buffer = new_buffer;
		buffer_ptr = buffer;
		end = buffer + len;
		return true;
	}

	void CheckResult() {
		auto result = PQgetResult(con.GetConn());
		if (!result || PQresultStatus(result) != PGRES_COMMAND_OK) {
			throw std::runtime_error("Failed to execute COPY: " + string(PQresultErrorMessage(result)));
		}
	}

	void Reset() {
		if (buffer) {
			PQfreemem(buffer);
		}
		buffer = nullptr;
		buffer_ptr = nullptr;
		end = nullptr;
	}
	bool Ready() {
		return buffer_ptr != nullptr;
	}

	void CheckHeader() {
		auto magic_len = PostgresConversion::COPY_HEADER_LENGTH;
		auto flags_len = 8;
		auto header_len = magic_len + flags_len;

		if (buffer_ptr + header_len >= end) {
			throw IOException("Unable to read binary COPY data from Postgres, invalid header");
		}
		if (memcmp(buffer_ptr, PostgresConversion::COPY_HEADER, magic_len) != 0) {
			throw IOException("Expected Postgres binary COPY header, got something else");
		}
		buffer_ptr += header_len;
		// as far as i can tell the "Flags field" and the "Header
		// extension area length" do not contain anything interesting
	}

public:
	template <class T>
	inline T ReadIntegerUnchecked() {
		T val = Load<T>(buffer_ptr);
		if (sizeof(T) == sizeof(uint8_t)) {
			// no need to flip single byte
		} else if (sizeof(T) == sizeof(uint16_t)) {
			val = ntohs(val);
		} else if (sizeof(T) == sizeof(uint32_t)) {
			val = ntohl(val);
		} else if (sizeof(T) == sizeof(uint64_t)) {
			val = ntohll(val);
		} else {
			D_ASSERT(0);
		}
		buffer_ptr += sizeof(T);
		return val;
	}

	bool OutOfBuffer() {
		return buffer_ptr >= end;
	}

	template <class T>
	inline T ReadInteger() {
		if (buffer_ptr + sizeof(T) > end) {
			throw IOException("Postgres scanner - out of buffer in ReadInteger");
		}
		return ReadIntegerUnchecked<T>();
	}

	inline bool ReadBoolean() {
		auto i = ReadInteger<uint8_t>();
		return i > 0;
	}

	inline float ReadFloat() {
		auto i = ReadInteger<uint32_t>();
		return *reinterpret_cast<float *>(&i);
	}

	inline double ReadDouble() {
		auto i = ReadInteger<uint64_t>();
		return *reinterpret_cast<double *>(&i);
	}

	inline date_t ReadDate() {
		auto jd = ReadInteger<uint32_t>();
		if (jd == POSTGRES_DATE_INF) {
			return date_t::infinity();
		}
		if (jd == POSTGRES_DATE_NINF) {
			return date_t::ninfinity();
		}
		return date_t(jd + POSTGRES_EPOCH_JDATE - DUCKDB_EPOCH_DATE); // magic!
	}

	inline dtime_t ReadTime() {
		return dtime_t(ReadInteger<uint64_t>());
	}

	inline dtime_tz_t ReadTimeTZ() {
		auto usec = ReadInteger<uint64_t>();
		auto tzoffset = ReadInteger<int32_t>();
		return dtime_tz_t(dtime_t(usec), -tzoffset);
	}

	inline timestamp_t ReadTimestamp() {
		auto usec = ReadInteger<uint64_t>();
		if (usec == POSTGRES_INFINITY) {
			return timestamp_t::infinity();
		}
		if (usec == POSTGRES_NINFINITY) {
			return timestamp_t::ninfinity();
		}
		return timestamp_t(usec + (POSTGRES_EPOCH_TS - DUCKDB_EPOCH_TS));
	}

	inline interval_t ReadInterval() {
		interval_t res;
		res.micros = ReadInteger<uint64_t>();
		res.days = ReadInteger<uint32_t>();
		res.months = ReadInteger<uint32_t>();
		return res;
	}

	inline hugeint_t ReadUUID() {
		hugeint_t res;
		auto upper = ReadInteger<uint64_t>();
		res.upper = upper ^ (int64_t(1) << 63);
		res.lower = ReadInteger<uint64_t>();
		return res;
	}

	const char *ReadString(idx_t string_length) {
		if (buffer_ptr + string_length > end) {
			throw IOException("Postgres scanner - out of buffer in ReadString");
		}
		auto result = const_char_ptr_cast(buffer_ptr);
		buffer_ptr += string_length;
		return result;
	}

	PostgresDecimalConfig ReadDecimalConfig() {
		PostgresDecimalConfig config;
		config.ndigits = ReadInteger<uint16_t>();
		config.weight = ReadInteger<int16_t>();
		auto sign = ReadInteger<uint16_t>();

		if (!(sign == NUMERIC_POS || sign == NUMERIC_NAN || sign == NUMERIC_PINF || sign == NUMERIC_NINF ||
		      sign == NUMERIC_NEG)) {
			throw NotImplementedException("Postgres numeric NA/Inf");
		}
		config.is_negative = sign == NUMERIC_NEG;
		config.scale = ReadInteger<uint16_t>();

		return config;
	}

	template <class T, class OP = DecimalConversionInteger>
	T ReadDecimal() {
		// this is wild
		auto config = ReadDecimalConfig();
		auto scale_POWER = OP::GetPowerOfTen(config.scale);

		if (config.ndigits == 0) {
			return 0;
		}
		T integral_part = 0, fractional_part = 0;

		if (config.weight >= 0) {
			integral_part = ReadInteger<uint16_t>();
			for (auto i = 1; i <= config.weight; i++) {
				integral_part *= NBASE;
				if (i < config.ndigits) {
					integral_part += ReadInteger<uint16_t>();
				}
			}
			integral_part *= scale_POWER;
		}

		// we need to find out how large the fractional part is in terms of powers
		// of ten this depends on how many times we multiplied with NBASE
		// if that is different from scale, we need to divide the extra part away
		// again
		// similarly, if trailing zeroes have been suppressed, we have not been multiplying t
		// the fractional part with NBASE often enough. If so, add additional powers
		if (config.ndigits > config.weight + 1) {
			auto fractional_power = (config.ndigits - config.weight - 1) * DEC_DIGITS;
			auto fractional_power_correction = fractional_power - config.scale;
			D_ASSERT(fractional_power_correction < 20);
			fractional_part = 0;
			for (int32_t i = MaxValue<int32_t>(0, config.weight + 1); i < config.ndigits; i++) {
				if (i + 1 < config.ndigits) {
					// more digits remain - no need to compensate yet
					fractional_part *= NBASE;
					fractional_part += ReadInteger<uint16_t>();
				} else {
					// last digit, compensate
					T final_base = NBASE;
					T final_digit = ReadInteger<uint16_t>();
					if (fractional_power_correction >= 0) {
						T compensation = OP::GetPowerOfTen(fractional_power_correction);
						final_base /= compensation;
						final_digit /= compensation;
					} else {
						T compensation = OP::GetPowerOfTen(-fractional_power_correction);
						final_base *= compensation;
						final_digit *= compensation;
					}
					fractional_part *= final_base;
					fractional_part += final_digit;
				}
			}
		}

		// finally
		auto base_res = OP::Finalize(config, integral_part + fractional_part);
		return (config.is_negative ? -base_res : base_res);
	}

	void ReadArray(const LogicalType &type, const PostgresType &postgres_type, Vector &out_vec, idx_t output_offset,
	               uint32_t current_count, uint32_t dimensions[], uint32_t ndim) {
		auto list_entries = FlatVector::GetData<list_entry_t>(out_vec);
		auto child_offset = ListVector::GetListSize(out_vec);
		auto child_dimension = dimensions[0];
		auto child_count = current_count * child_dimension;
		// set up the list entries for this dimension
		auto current_offset = child_offset;
		for (idx_t c = 0; c < current_count; c++) {
			auto &list_entry = list_entries[output_offset + c];
			list_entry.offset = current_offset;
			list_entry.length = child_dimension;
			current_offset += child_dimension;
		}
		ListVector::Reserve(out_vec, child_offset + child_count);
		auto &child_vec = ListVector::GetEntry(out_vec);
		auto &child_type = ListType::GetChildType(type);
		auto &child_pg_type = postgres_type.children[0];
		if (ndim > 1) {
			// there are more dimensions to read - recurse into child list
			ReadArray(child_type, child_pg_type, child_vec, child_offset, child_count, dimensions + 1, ndim - 1);
		} else {
			// this is the last level - read the actual values
			for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
				ReadValue(child_type, child_pg_type, child_vec, child_offset + child_idx);
			}
		}
		ListVector::SetListSize(out_vec, child_offset + child_count);
	}

	void ReadValue(const LogicalType &type, const PostgresType &postgres_type, Vector &out_vec, idx_t output_offset) {
		auto value_len = ReadInteger<int32_t>();
		if (value_len == -1) { // NULL
			FlatVector::SetNull(out_vec, output_offset, true);
			return;
		}
		switch (type.id()) {
		case LogicalTypeId::SMALLINT:
			D_ASSERT(value_len == sizeof(int16_t));
			FlatVector::GetData<int16_t>(out_vec)[output_offset] = ReadInteger<int16_t>();
			break;
		case LogicalTypeId::INTEGER:
			D_ASSERT(value_len == sizeof(int32_t));
			FlatVector::GetData<int32_t>(out_vec)[output_offset] = ReadInteger<int32_t>();
			break;
		case LogicalTypeId::UINTEGER:
			D_ASSERT(value_len == sizeof(uint32_t));
			FlatVector::GetData<uint32_t>(out_vec)[output_offset] = ReadInteger<uint32_t>();
			break;
		case LogicalTypeId::BIGINT:
			if (postgres_type.info == PostgresTypeAnnotation::CTID) {
				D_ASSERT(value_len == 6);
				int64_t page_index = ReadInteger<int32_t>();
				int64_t row_in_page = ReadInteger<int16_t>();
				FlatVector::GetData<int64_t>(out_vec)[output_offset] = (page_index << 16LL) + row_in_page;
				return;
			}
			D_ASSERT(value_len == sizeof(int64_t));
			FlatVector::GetData<int64_t>(out_vec)[output_offset] = ReadInteger<int64_t>();
			break;
		case LogicalTypeId::FLOAT:
			D_ASSERT(value_len == sizeof(float));
			FlatVector::GetData<float>(out_vec)[output_offset] = ReadFloat();
			break;
		case LogicalTypeId::DOUBLE: {
			// this was an unbounded decimal, read params from value and cast to double
			if (postgres_type.info == PostgresTypeAnnotation::NUMERIC_AS_DOUBLE) {
				FlatVector::GetData<double>(out_vec)[output_offset] = ReadDecimal<double, DecimalConversionDouble>();
				break;
			}
			D_ASSERT(value_len == sizeof(double));
			FlatVector::GetData<double>(out_vec)[output_offset] = ReadDouble();
			break;
		}

		case LogicalTypeId::BLOB:
		case LogicalTypeId::VARCHAR: {
			if (postgres_type.info == PostgresTypeAnnotation::JSONB) {
				auto version = ReadInteger<uint8_t>();
				value_len--;
				if (version != 1) {
					throw NotImplementedException("JSONB version number mismatch, expected 1, got %d", version);
				}
			}
			auto str = ReadString(value_len);
			if (postgres_type.info == PostgresTypeAnnotation::FIXED_LENGTH_CHAR) {
				// CHAR column - remove trailing spaces
				while (value_len > 0 && str[value_len - 1] == ' ') {
					value_len--;
				}
			}
			FlatVector::GetData<string_t>(out_vec)[output_offset] =
			    StringVector::AddStringOrBlob(out_vec, str, value_len);
			break;
		}
		case LogicalTypeId::BOOLEAN:
			D_ASSERT(value_len == sizeof(bool));
			FlatVector::GetData<bool>(out_vec)[output_offset] = ReadBoolean();
			break;
		case LogicalTypeId::DECIMAL: {
			if (value_len < sizeof(uint16_t) * 4) {
				throw InvalidInputException("Need at least 8 bytes to read a Postgres decimal. Got %d", value_len);
			}
			switch (type.InternalType()) {
			case PhysicalType::INT16:
				FlatVector::GetData<int16_t>(out_vec)[output_offset] = ReadDecimal<int16_t>();
				break;
			case PhysicalType::INT32:
				FlatVector::GetData<int32_t>(out_vec)[output_offset] = ReadDecimal<int32_t>();
				break;
			case PhysicalType::INT64:
				FlatVector::GetData<int64_t>(out_vec)[output_offset] = ReadDecimal<int64_t>();
				break;
			case PhysicalType::INT128:
				FlatVector::GetData<hugeint_t>(out_vec)[output_offset] =
				    ReadDecimal<hugeint_t, DecimalConversionHugeint>();
				break;
			default:
				throw InvalidInputException("Unsupported decimal storage type");
			}
			break;
		}

		case LogicalTypeId::DATE: {
			D_ASSERT(value_len == sizeof(int32_t));
			auto out_ptr = FlatVector::GetData<date_t>(out_vec);
			out_ptr[output_offset] = ReadDate();
			break;
		}
		case LogicalTypeId::TIME: {
			D_ASSERT(value_len == sizeof(int64_t));
			FlatVector::GetData<dtime_t>(out_vec)[output_offset] = ReadTime();
			break;
		}
		case LogicalTypeId::TIME_TZ: {
			D_ASSERT(value_len == sizeof(int64_t) + sizeof(int32_t));
			FlatVector::GetData<dtime_tz_t>(out_vec)[output_offset] = ReadTimeTZ();
			break;
		}
		case LogicalTypeId::TIMESTAMP_TZ:
		case LogicalTypeId::TIMESTAMP: {
			D_ASSERT(value_len == sizeof(int64_t));
			FlatVector::GetData<timestamp_t>(out_vec)[output_offset] = ReadTimestamp();
			break;
		}
		case LogicalTypeId::ENUM: {
			auto enum_val = string(ReadString(value_len), value_len);
			auto offset = EnumType::GetPos(type, enum_val);
			if (offset < 0) {
				throw IOException("Could not map ENUM value %s", enum_val);
			}
			switch (type.InternalType()) {
			case PhysicalType::UINT8:
				FlatVector::GetData<uint8_t>(out_vec)[output_offset] = (uint8_t)offset;
				break;
			case PhysicalType::UINT16:
				FlatVector::GetData<uint16_t>(out_vec)[output_offset] = (uint16_t)offset;
				break;

			case PhysicalType::UINT32:
				FlatVector::GetData<uint32_t>(out_vec)[output_offset] = (uint32_t)offset;
				break;

			default:
				throw InternalException("ENUM can only have unsigned integers (except "
				                        "UINT64) as physical types, got %s",
				                        TypeIdToString(type.InternalType()));
			}
			break;
		}
		case LogicalTypeId::INTERVAL: {
			FlatVector::GetData<interval_t>(out_vec)[output_offset] = ReadInterval();
			break;
		}
		case LogicalTypeId::UUID: {
			D_ASSERT(value_len == 2 * sizeof(int64_t));
			FlatVector::GetData<hugeint_t>(out_vec)[output_offset] = ReadUUID();
			break;
		}
		case LogicalTypeId::LIST: {
			auto &list_entry = FlatVector::GetData<list_entry_t>(out_vec)[output_offset];
			auto child_offset = ListVector::GetListSize(out_vec);

			if (value_len < 1) {
				list_entry.offset = child_offset;
				list_entry.length = 0;
				break;
			}
			D_ASSERT(value_len >= 3 * sizeof(uint32_t));
			auto array_dim = ReadInteger<uint32_t>();
			auto array_has_null = ReadInteger<uint32_t>(); // whether or not the array has nulls - ignore
			auto value_oid = ReadInteger<uint32_t>();      // value_oid - not necessary
			if (array_dim == 0) {
				list_entry.offset = child_offset;
				list_entry.length = 0;
				return;
			}
			// verify the number of dimensions matches the expected number of dimensions
			idx_t expected_dimensions = 0;
			const_reference<LogicalType> current_type = type;
			while (current_type.get().id() == LogicalTypeId::LIST) {
				current_type = ListType::GetChildType(current_type.get());
				expected_dimensions++;
			}
			if (expected_dimensions != array_dim) {
				throw InvalidInputException(
				    "Expected an array with %llu dimensions, but this array has %llu dimensions. The array stored in "
				    "Postgres does not match the schema. Postgres does not enforce that arrays match the provided "
				    "schema but DuckDB requires this.\nSet pg_array_as_varchar=true to read the array as a varchar "
				    "instead. Note that you might have to run CALL pg_clear_cache() to clear cached type information "
				    "as well.",
				    expected_dimensions, array_dim);
			}
			auto dimensions = unique_ptr<uint32_t[]>(new uint32_t[array_dim]);
			for (idx_t d = 0; d < array_dim; d++) {
				dimensions[d] = ReadInteger<uint32_t>();
				auto lb = ReadInteger<uint32_t>(); // index lower bounds for each dimension -- we don't need them
			}
			// read the arrays recursively
			ReadArray(type, postgres_type, out_vec, output_offset, 1, dimensions.get(), array_dim);
			break;
		}
		case LogicalTypeId::STRUCT: {
			auto &child_entries = StructVector::GetEntries(out_vec);
			auto entry_count = ReadInteger<uint32_t>();
			if (entry_count != child_entries.size()) {
				throw InternalException("Mismatch in entry count: expected %d but got %d", child_entries.size(),
				                        entry_count);
			}
			for (idx_t c = 0; c < entry_count; c++) {
				auto &child = *child_entries[c];
				auto value_oid = ReadInteger<uint32_t>();
				ReadValue(child.GetType(), postgres_type.children[c], child, output_offset);
			}
			break;
		}
		default:
			throw InternalException("Unsupported Type %s", type.ToString());
		}
	}

private:
	data_ptr_t buffer = nullptr;
	data_ptr_t buffer_ptr = nullptr;
	data_ptr_t end = nullptr;
	PostgresConnection &con;
};

} // namespace duckdb
