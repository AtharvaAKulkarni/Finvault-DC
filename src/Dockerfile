FROM eclipse-temurin:17-jdk

WORKDIR /app

# Install wget and sqlite3
RUN apt-get update && apt-get install -y wget sqlite3 && rm -rf /var/lib/apt/lists/*

# Download SQLite JDBC driver and SLF4J dependencies
RUN wget https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.45.1.0/sqlite-jdbc-3.45.1.0.jar -O sqlite-jdbc.jar
RUN wget https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.12/slf4j-api-2.0.12.jar -O slf4j-api.jar
RUN wget https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.12/slf4j-simple-2.0.12.jar -O slf4j-simple.jar

# Copy source files
COPY . /app/

# Compile with all JARs in classpath
RUN find . -name "*.java" > sources.txt && javac -cp ".:*" @sources.txt

# Default command using wildcard classpath
CMD ["java", "-cp", ".:*", "FinanceServer"]