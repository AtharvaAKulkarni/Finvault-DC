FROM eclipse-temurin:17-jdk

WORKDIR /app

# Copy all files and package subdirectories into /app
COPY . /app/

# Find and compile all .java files recursively across packages
RUN find . -name "*.java" > sources.txt && javac @sources.txt

# Default command (overridden by docker-compose)
CMD ["java", "FinanceServer"]