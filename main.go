package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
)

var session *gocql.Session
func initializeDB() error {
    // Create keyspace if it doesn't exist
    if err := session.Query(`CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`).Exec(); err != nil {
        return err
    }

    // Create users table if it doesn't exist
    if err := session.Query(`CREATE TABLE IF NOT EXISTS my_keyspace.users (id TEXT PRIMARY KEY, username TEXT)`).Exec(); err != nil {
        return err
    }

    return nil
}

func init() {
	// Connect to ScyllaDB
	cluster := gocql.NewCluster("localhost")
	cluster.Keyspace = "system" // Use the 'system' keyspace to check if 'my_keyspace' exists
	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	// Check if 'my_keyspace' already exists
	var keyspaceExists bool
    var keyspaceName string
    iter := session.Query("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?", "my_keyspace").Iter()
    for iter.Scan(&keyspaceName) {
        keyspaceExists = true
    }
    if err := iter.Close(); err != nil {
        panic(err)
    }


	// Create 'my_keyspace' if it doesn't exist
	if !keyspaceExists {
		if err := session.Query("CREATE KEYSPACE my_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}").Exec(); err != nil {
			panic(err)
		}
	}
    // Initialize database
    if err := initializeDB(); err != nil {
        panic(err)
    }
	// Close the session
	session.Close()
}

func main() {
	// Reconnect to ScyllaDB using the 'my_keyspace' keyspace
	cluster := gocql.NewCluster("localhost")
	cluster.Keyspace = "my_keyspace"
	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// Initialize Gin router
	router := gin.Default()

	// Define GET endpoint
	router.GET("/users/:id", getUser)

	// Define POST endpoint
	router.POST("/users", createUser)

	// Start the server
	if err := router.Run(":8080"); err != nil {
		panic(fmt.Errorf("failed to start server: %w", err))
	}
}

func getUser(c *gin.Context) {
	// Get user ID from path parameter
	userID := c.Param("id")

	// Query user from the database
	var username string
	if err := session.Query("SELECT username FROM users WHERE id = ?", userID).Scan(&username); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}

	// Return user details
	c.JSON(http.StatusOK, gin.H{"id": userID, "username": username})
}

func createUser(c *gin.Context) {
    // Parse JSON request body
    var user struct {
        ID       string `json:"id"`
        Username string `json:"username"`
    }
    if err := c.ShouldBindJSON(&user); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    // Check if user ID or username is empty
    if user.ID == "" || user.Username == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "user ID and username cannot be empty"})
        return
    }

    // Insert user into the database
    query := session.Query("INSERT INTO users (id, username) VALUES (?, ?)", user.ID, user.Username)
    if err := query.Exec(); err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create user"})
        log.Println("Error inserting user:", err)
        return
    }

    // Return success message
    c.JSON(http.StatusCreated, gin.H{"message": "user created"})
}