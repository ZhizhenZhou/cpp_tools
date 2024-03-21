# 编译选项
CXX = g++
CXXFLAGS = -std=c++17 -I. -fPIC

# 链接选项
LDFLAGS = -lpqxx

# 目标文件
OBJS = radix_convert.o linktable.o txtHelper.o dbHelper.o csv.o

# 目标可执行文件
TARGET = mylib.so

# 默认目标
all: $(TARGET)

# 生成目标文件
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 生成目标可执行文件
$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -shared -o $@ $^ $(LDFLAGS)

# 清理中间文件和目标文件
clean:
	rm -f $(OBJS) $(TARGET)
