//
// Created by ila on 2/24/25.
//

#include "client_functions.hpp"

#include <common.hpp>
#include <netdb.h>
#include <unistd.h>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/main/connection.hpp>
#include <netinet/in.h>
#include <sys/socket.h>

namespace duckdb {

void CloseConnection(int32_t sock) {
	// close the connection
	auto message = close_connection;
	send(sock, &message, sizeof(int32_t), 0);
	close(sock);
	shutdown(sock, SHUT_RDWR);
}

int32_t ConnectClient(unordered_map<string, string> &config) {

	sockaddr_in serv_addr;

	int sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0) {
		Printer::Print("Socket creation error! \n");
	}

	hostent *h = gethostbyname(config["server_addr"].c_str());
	if (h == nullptr) { // lookup the hostname
		Printer::Print("Unknown host\n");
	}

	memset(&serv_addr, '\0', sizeof(serv_addr)); // zero structure out
	serv_addr.sin_family = AF_INET; // match the socket() call
	memcpy(&serv_addr.sin_addr.s_addr, h->h_addr_list[0], h->h_length); // copy the address
	serv_addr.sin_port = htons(stoi(config["server_port"]));

	const int client_fd = connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
	if (client_fd < 0) {
		Printer::Print("\nConnection failed: " + to_string(client_fd) + ", error " + to_string(errno) + "\n");
	}
	return sock;
}

void SendChunks(std::unique_ptr<MaterializedQueryResult> &result, int32_t sock) {

	auto &collection = result->Collection();
	idx_t num_chunks = collection.ChunkCount();
	send(sock, &num_chunks, sizeof(idx_t), 0);

	for (auto &chunk : collection.Chunks()) {
		MemoryStream target;
		BinarySerializer serializer(target);
		serializer.Begin();
		chunk.Serialize(serializer);
		serializer.End();

		auto data = target.GetData();
		idx_t len = target.GetPosition();

		send(sock, &len, sizeof(ssize_t), 0);
		send(sock, data, len, 0);
	}
}

timestamp_t RefreshMaterializedView(string &view_name, Connection &con) {

	auto r = con.Query("pragma ivm('" + view_name + "');");
	if (r->HasError()) {
		throw ParserException("Error while refreshing materialized view: " + r->GetError());
	}

	auto timestamp = Timestamp::GetCurrentTimestamp();
	return timestamp; // todo - timezone

}

int32_t SendResults(string &view_name, timestamp_t timestamp, Connection &con, string &path) {
	string config_file = "client.config";
	auto config = ParseConfig(path, config_file);
	auto chunks = con.Query("select * from " + view_name + ";");
	if (chunks->HasError()) {
		throw ParserException("Error while fetching chunks: " + chunks->GetError());
	}
	if (chunks->Collection().Count() == 0) {
		throw ParserException("No data to send!");
	}

	int32_t sock = ConnectClient(config);

	auto message = new_result;
	send(sock, &message, sizeof(int32_t), 0);

	// sending client id
	auto client = con.Query("select id from client_information");
	auto client_id = client->Fetch()->GetValue(0, 0).GetValue<uint64_t>();
	send(sock, &client_id, sizeof(uint64_t), 0);

	// receive "ok" or "error"
	int32_t response;
	read(sock, &response, sizeof(int32_t));
	if (response != ok) {
		CloseConnection(sock);
		throw ParserException("Client not initialized!");
	}

	// send view name
	auto size = view_name.size();
	send(sock, &size, sizeof(view_name.size()), 0);
	send(sock, view_name.c_str(), view_name.size(), 0);

	auto r = con.Query("insert into client_refreshes values ('" + view_name + "', '" + Timestamp::ToString(timestamp) + "') on conflict do update set last_result = '" + Timestamp::ToString(timestamp) + "' where view_name = '" + view_name + "';");
	if (r->HasError()) {
		throw ParserException("Error while updating client metadata: " + r->GetError());
	}

	// also send the timestamp (in case of network delays)
	string timestamp_string = Timestamp::ToString(timestamp);
	auto timestamp_size = timestamp_string.size();
	send(sock, &timestamp_size, sizeof(size_t), 0);
	send(sock, timestamp_string.c_str(), timestamp_size, 0);

	// sending (todo encrypted) data
	SendChunks(chunks, sock);

	Printer::Print("Sent data to the server!");

	// closing the connected socket
	return sock;
}

}
