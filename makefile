# 编译选项
CXX = g++
CXXFLAGS = -std=c++17 -I. -fPIC

# 链接选项
LDFLAGS = -lpqxx

# 目标文件
OBJS = dbHelper.o sc_search.o sc_createtable.o

# 目标可执行文件
TARGET_SC = sc.so
#TARGET_SC_SEARCH = sc_search
#TARGET_SC_CREATETABLE = sc_createtable

# 默认目标
all: $(TARGET_SC)
#all: $(TARGET_SC_CREATETABLE)
#all: $(TARGET_SC) $(TARGET_SC_SEARCH) $(TARGET_SC_CREATETABLE)

# 生成目标文件
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 生成目标可执行文件
$(TARGET_SC): dbHelper.o sc_search.o sc_createtable.o
	$(CXX) $(CXXFLAGS) -shared -o $@ $^ $(LDFLAGS)

#$(TARGET_SC_SEARCH): dbHelper.o sc_search.o
#	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

#$(TARGET_SC_CREATETABLE): dbHelper.o sc_createtable.o
#	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# 清理中间文件和目标文件
clean:
#	rm -f $(OBJS) $(TARGET_SC) $(TARGET_SC_SEARCH) $(TARGET_SC_CREATETABLE)
	rm -f $(OBJS) $(TARGET_SC)
