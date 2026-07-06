package main


import (
    "encoding/json"
    "log"
    "net/http"
    "os"
)

func getEnv(key, fallback string) string {
    if v := os.Getenv(key); v != "" {
        return v
   
}

    return fallback
}

func main() {
    mux := http.NewServeMux()
    mux.HandleFunc("/health",
func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        _ = json.NewEncoder(w).Encode(map[string]string{"status":"healthy","service":"fraud-detection-service"})
   
}
)

    addr := ":" + getEnv("PORT", "9102")
    log.Printf("fraud-detection-service started on %s", addr)
    if err := http.ListenAndServe(addr, mux); err != nil {
        log.Fatal(err)
   
}

}
