package main

import (
	"crypto"
	cryptoRand "crypto/rand" // Aliased import for cryptographic operations
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	mathRand "math/rand" // Aliased import for non-secure random number generation
	"os"
	"time"
)

func main() {
	//rw()
	sig()
}

func rw() {
	mathRand.Seed(time.Now().UnixNano()) // Initialize the seed for non-secure random number generation
	// Open or create the database
	db, err := leveldb.OpenFile("./leveldb", nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Slice for storing keys to test read operations
	//var testKeys []string

	// Populate the database and save some keys for read testing
	for i := 0; i < 100000000; i++ { // Reduced the count for demonstration
		key := generateRandomHash(32)
		value := generateRandomHash(32)
		err := db.Put([]byte(key), []byte(value), nil)
		if err != nil {
			panic(err)
		}

		/* Save a key for testing read operations for every 10000 keys
		if i%10000 == 0 {
			testKeys = append(testKeys, key)
		}*/
	}

	/*Test read and write operations
	start := time.Now()
	for i := 0; i < 10000; i++ {
		// Randomly select a saved key for read testing
		readKey := testKeys[mathRand.Intn(len(testKeys))]
		_, err := db.Get([]byte(readKey), nil)
		if err != nil {
			panic(err)
		}

		// Write a new data entry
		writeKey := generateRandomHash(32)
		writeValue := generateRandomHash(32)
		err = db.Put([]byte(writeKey), []byte(writeValue), nil)
		if err != nil {
			panic(err)
		}
	}
	duration := time.Since(start)
	fmt.Printf("10000 read-write operations took: %v\n", duration)

	fmt.Println("LevelDB read-write test completed.")*/
}

// Function to generate a random hash string
func generateRandomHash(length int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, length)
	for i := range b {
		b[i] = letters[mathRand.Intn(len(letters))]
	}
	return string(b)
}

// Function to test signature generation and verification
func sig() {
	// Generate an RSA private key
	privateKey, err := rsa.GenerateKey(cryptoRand.Reader, 2048)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to generate private key: %v\n", err)
		return
	}
	publicKey := &privateKey.PublicKey

	// Data to sign
	message := "Hello, RSA signing!"
	hashed := sha256.Sum256([]byte(message))

	// Generate signature
	signature, err := rsa.SignPKCS1v15(cryptoRand.Reader, privateKey, crypto.SHA256, hashed[:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to sign message: %v\n", err)
		return
	}
	fmt.Printf("Signature: %s\n", base64.StdEncoding.EncodeToString(signature))

	// Verify the signature 100 times
	startVerify := time.Now()
	for i := 0; i < 10000; i++ {
		err = rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, hashed[:], signature)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to verify signature: %v\n", err)
			return
		}
	}
	durationVerify := time.Since(startVerify)
	fmt.Printf("10000 verifications took: %v\n", durationVerify)
	fmt.Println("Average verification time: ", durationVerify/time.Duration(10000))
}
