// 컴파일: g++ -std=c++17 -o disk /home/system/tpc/TPC-H\ V3.0.1/2025-database-system/disk/disk.cpp
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <cstdint>
#include <sstream>
#include <iomanip>

class BlockStorage {
private:
    static const size_t BLOCK_SIZE = 4096;  // 4KB 블록
    static const size_t HEADER_SIZE = 8;     // 블록 헤더: 레코드 수(4) + 사용 바이트(4)
    
    std::string dataFile;
    
    struct Record {
        std::string nationkey;
        std::string name;
        std::string regionkey;
        std::string comment;
        
        Record(const std::string& k, const std::string& n, 
               const std::string& r, const std::string& c)
            : nationkey(k), name(n), regionkey(r), comment(c) {}
    };
    
    // 레코드를 바이트로 인코딩
    std::vector<uint8_t> encodeRecord(const Record& record) {
        std::vector<uint8_t> encoded;
        std::vector<std::string> fields = {
            record.nationkey, record.name, 
            record.regionkey, record.comment
        };
        
        // 각 필드를 인코딩: 2바이트 길이 + 데이터
        std::vector<uint8_t> fieldData;
        for (const auto& field : fields) {
            uint16_t len = field.length();
            fieldData.push_back(len & 0xFF);
            fieldData.push_back((len >> 8) & 0xFF);
            fieldData.insert(fieldData.end(), field.begin(), field.end());
        }
        
        // 전체 레코드: 4바이트 길이 + 필드 데이터
        uint32_t totalLen = fieldData.size();
        encoded.push_back(totalLen & 0xFF);
        encoded.push_back((totalLen >> 8) & 0xFF);
        encoded.push_back((totalLen >> 16) & 0xFF);
        encoded.push_back((totalLen >> 24) & 0xFF);
        encoded.insert(encoded.end(), fieldData.begin(), fieldData.end());
        
        return encoded;
    }
    
    // 바이트에서 레코드 디코딩
    Record decodeRecord(const uint8_t* data, size_t& offset) {
        // 레코드 길이 읽기
        uint32_t recordLength = data[offset] | 
                               (data[offset + 1] << 8) |
                               (data[offset + 2] << 16) |
                               (data[offset + 3] << 24);
        offset += 4;
        
        std::vector<std::string> fields;
        size_t endOffset = offset + recordLength;
        
        // 각 필드 읽기
        while (offset < endOffset) {
            uint16_t fieldLen = data[offset] | (data[offset + 1] << 8);
            offset += 2;
            
            std::string field(reinterpret_cast<const char*>(&data[offset]), fieldLen);
            fields.push_back(field);
            offset += fieldLen;
        }
        
        return Record(
            fields.size() > 0 ? fields[0] : "",
            fields.size() > 1 ? fields[1] : "",
            fields.size() > 2 ? fields[2] : "",
            fields.size() > 3 ? fields[3] : ""
        );
    }
    
public:
    BlockStorage(const std::string& file) : dataFile(file) {}
    
    // TBL 파일 파싱
    std::vector<Record> parseTblFile(const std::string& tblFile) {
        std::vector<Record> records;
        std::ifstream file(tblFile);
        
        if (!file.is_open()) {
            std::cerr << "파일을 열 수 없습니다: " << tblFile << std::endl;
            return records;
        }
        
        std::string line;
        while (std::getline(file, line)) {
            if (line.empty()) continue;
            
            std::vector<std::string> parts;
            std::stringstream ss(line);
            std::string part;
            
            while (std::getline(ss, part, '|')) {
                parts.push_back(part);
            }
            
            if (parts.size() >= 4) {
                records.emplace_back(parts[0], parts[1], parts[2], parts[3]);
            }
        }
        
        file.close();
        return records;
    }
    
    // 레코드들을 블록으로 저장
    void writeRecordsToBlocks(const std::vector<Record>& records) {
        std::ofstream file(dataFile, std::ios::binary);
        
        if (!file.is_open()) {
            std::cerr << "파일을 쓸 수 없습니다: " << dataFile << std::endl;
            return;
        }
        
        std::vector<uint8_t> currentBlock(BLOCK_SIZE, 0);
        uint32_t recordCount = 0;
        size_t bytesUsed = HEADER_SIZE;
        
        for (const auto& record : records) {
            std::vector<uint8_t> encoded = encodeRecord(record);
            
            // 현재 블록에 공간이 충분한지 확인
            if (bytesUsed + encoded.size() > BLOCK_SIZE) {
                // 블록 헤더 작성
                std::memcpy(&currentBlock[0], &recordCount, 4);
                uint32_t used = bytesUsed;
                std::memcpy(&currentBlock[4], &used, 4);
                
                // 블록 쓰기
                file.write(reinterpret_cast<char*>(currentBlock.data()), BLOCK_SIZE);
                
                // 새 블록 시작
                std::fill(currentBlock.begin(), currentBlock.end(), 0);
                recordCount = 0;
                bytesUsed = HEADER_SIZE;
            }
            
            // 레코드를 블록에 추가
            std::memcpy(&currentBlock[bytesUsed], encoded.data(), encoded.size());
            bytesUsed += encoded.size();
            recordCount++;
        }
        
        // 마지막 블록 쓰기
        if (recordCount > 0) {
            std::memcpy(&currentBlock[0], &recordCount, 4);
            uint32_t used = bytesUsed;
            std::memcpy(&currentBlock[4], &used, 4);
            file.write(reinterpret_cast<char*>(currentBlock.data()), BLOCK_SIZE);
        }
        
        file.close();
        std::cout << "저장 완료: " << dataFile << std::endl;
    }
    
    // 블록 파일에서 모든 레코드 읽기
    std::vector<Record> readAllRecords() {
        std::vector<Record> records;
        std::ifstream file(dataFile, std::ios::binary);
        
        if (!file.is_open()) {
            std::cerr << "파일을 열 수 없습니다: " << dataFile << std::endl;
            return records;
        }
        
        std::vector<uint8_t> block(BLOCK_SIZE);
        int blockNum = 0;
        
        while (file.read(reinterpret_cast<char*>(block.data()), BLOCK_SIZE)) {
            // 블록 헤더 읽기
            uint32_t recordCount, bytesUsed;
            std::memcpy(&recordCount, &block[0], 4);
            std::memcpy(&bytesUsed, &block[4], 4);
            
            std::cout << "블록 " << blockNum << ": " 
                      << recordCount << "개 레코드, " 
                      << bytesUsed << "바이트 사용" << std::endl;
            
            // 레코드들 읽기
            size_t offset = HEADER_SIZE;
            for (uint32_t i = 0; i < recordCount; i++) {
                Record record = decodeRecord(block.data(), offset);
                records.push_back(record);
            }
            
            blockNum++;
        }
        
        file.close();
        return records;
    }
    
    // 통계 출력
    void printStatistics() {
        std::ifstream file(dataFile, std::ios::binary | std::ios::ate);
        if (!file.is_open()) return;
        
        size_t fileSize = file.tellg();
        file.close();
        
        size_t numBlocks = fileSize / BLOCK_SIZE;
        
        std::cout << "\n=== 블록 저장 통계 ===" << std::endl;
        std::cout << "파일 크기: " << fileSize << " 바이트" << std::endl;
        std::cout << "블록 크기: " << BLOCK_SIZE << " 바이트" << std::endl;
        std::cout << "총 블록 수: " << numBlocks << std::endl;
        std::cout << std::fixed << std::setprecision(1);
        std::cout << "블록당 평균 사용률: " 
                  << (fileSize / (double)(numBlocks * BLOCK_SIZE) * 100) 
                  << "%" << std::endl;
    }
};

int main() {
    // TBL 파일 경로
    std::string tblFile = "../tbl/partsupp.tbl";
    std::string dataFile = "../bin/blocks.dat";
    
    BlockStorage storage(dataFile);
    
    // 1. TBL 파일 읽기
    std::cout << "=== TBL 파일 읽기 ===" << std::endl;
    auto records = storage.parseTblFile(tblFile);
    std::cout << "읽은 레코드 수: " << records.size() << std::endl;
    if (!records.empty()) {
        std::cout << "첫 번째 레코드: " 
                  << records[0].nationkey << "|"
                  << records[0].name << "|"
                  << records[0].regionkey << "|"
                  << records[0].comment << std::endl;
    }
    
    // 2. 블록으로 저장
    std::cout << "\n=== 블록으로 저장 ===" << std::endl;
    storage.writeRecordsToBlocks(records);
    
    // 3. 통계 출력
    storage.printStatistics();
    
    // 4. 블록에서 읽기
    std::cout << "\n=== 블록에서 읽기 ===" << std::endl;
    auto loadedRecords = storage.readAllRecords();
    std::cout << "읽은 레코드 수: " << loadedRecords.size() << std::endl;
    
    // 5. 검증
    std::cout << "\n=== 검증 ===" << std::endl;
    bool match = (records.size() == loadedRecords.size());
    if (match) {
        for (size_t i = 0; i < records.size(); i++) {
            if (records[i].nationkey != loadedRecords[i].nationkey ||
                records[i].name != loadedRecords[i].name ||
                records[i].regionkey != loadedRecords[i].regionkey ||
                records[i].comment != loadedRecords[i].comment) {
                match = false;
                break;
            }
        }
    }
    
    if (match) {
        std::cout << "✓ 모든 레코드가 올바르게 저장/복원되었습니다!" << std::endl;
    } else {
        std::cout << "✗ 데이터 불일치!" << std::endl;
    }
    
    // 일부 레코드 출력
    std::cout << "\n=== 복원된 레코드 샘플 ===" << std::endl;
    for (size_t i = 0; i < std::min(size_t(5), loadedRecords.size()); i++) {
        std::cout << i << ": " 
                  << loadedRecords[i].nationkey << "|"
                  << loadedRecords[i].name << "|"
                  << loadedRecords[i].regionkey << "|"
                  << loadedRecords[i].comment << std::endl;
    }
    
    return 0;
}