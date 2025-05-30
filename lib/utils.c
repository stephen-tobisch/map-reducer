#include "utils.h"
#include <dirent.h>
#include <string.h>

char *getChunkData(int mapperID) {
    int msg_id;
    key_t key = ftok(".", 5473708);
    struct msgBuffer msg;

    if ((msg_id = msgget(key, 0666 | IPC_CREAT)) == -1) {
        printf("Message get error.\n");
        exit(1);
    }

    // receive message from mapper
    if ((msgrcv(msg_id, (void *)&msg, sizeof(msg.msgText), mapperID, 0)) == -1) {
        printf("Message recieved error.\n");
        exit(1);
    }

    // check if it's the last message
    if (strcmp(msg.msgText, "END") == 0) {
        return NULL;
    }

    // returns message as a char pointer which is allocated in memory
    char *msgText = (char *)malloc(sizeof(char) * sizeof(msg.msgText));
    memset(msgText, '\0', MSGSIZE);
    strcpy(msgText, msg.msgText);
    return msgText;
}

// sends chunks of size 1024 to the mappers in RR fashion
void sendChunkData(char *inputFile, int nMappers) {
    int msg_id;
    int counter = 1;
    struct msgBuffer msg;
    key_t key = ftok(".", 5473708);
    // holds one char at a time from inputFile
    char buf[10];
    // holds one word at a time
    char word[chunkSize];

    memset(msg.msgText, '\0', MSGSIZE);

    if ((msg_id = msgget(key, 0666 | IPC_CREAT)) == -1) {
        printf("Message get error.\n");
        exit(1);
    }

    FILE *fptr = fopen(inputFile, "r");

    // parses through inputFile one char at a time
    while (fscanf(fptr, "%c", buf) == 1) {
        // add char to word
        strcat(word, buf);
        // check if end of word
        if (buf[0] == ' ' || buf[0] == '\n') {
            // check if word should be added to this message or the next
            if (strlen(msg.msgText) + strlen(word) <= 1024) {
                // add word to current chunk and reset word
                strcat(msg.msgText, word);
                memset(word, '\0', chunkSize);
            } else {
                // sends current chunk if current word won't fit to mapper in round robin fashion
                // counter determines which mapper to send to
                msg.msgType = counter;
                counter++;
                if (msgsnd(msg_id, (void *)&msg, sizeof(msg.msgText), 0) == -1) {
                    printf("Message send error.\n");
                    exit(1);
                }
                if (counter > nMappers) {
                    counter = 1;
                }

                memset(msg.msgText, '\0', MSGSIZE);
            }
        }
    }

    // adds last word in file to current chunk
    if (strlen(msg.msgText) + strlen(word) <= 1024) {
        strcat(msg.msgText, word);
    }

    // send last chunk
    msg.msgType = counter;
    if (msgsnd(msg_id, (void *)&msg, sizeof(msg.msgText), 0) == -1) {
        printf("Message send error.\n");
        exit(1);
    }

    // send "END" message to each mapper
    for (int i = 0; i < nMappers; i++) {
        msg.msgType = i + 1;
        memset(msg.msgText, '\0', MSGSIZE);
        strcpy(msg.msgText, "END");
        if (msgsnd(msg_id, (void *)&msg, sizeof(msg.msgText), 0) == -1) {
            printf("Message send error.\n");
            exit(1);
        }
    }
}

// hash function to divide the list of word.txt files across reducers
// http://www.cse.yorku.ca/~oz/hash.html
int hashFunction(char *key, int reducers) {
    unsigned long hash = 0;
    int c;

    while ((c = *key++) != '\0')
        hash = c + (hash << 6) + (hash << 16) - hash;

    return (hash % reducers);
}

int getInterData(char *key, int reducerID) {
    int msg_id;
    key_t msg_key = ftok(".", 5473708);
    struct msgBuffer msg;

    if ((msg_id = msgget(msg_key, 0666 | IPC_CREAT)) == -1) {
        printf("Message get failed.\n");
        exit(1);
    }

    // receive .txt path from reducer
    if ((msgrcv(msg_id, (void *)&msg, sizeof(msg.msgText), reducerID, 0)) == -1) {
        printf("Message recieved error.\n");
        exit(1);
    }

    // check if last message
    if (strcmp(msg.msgText, "END") == 0) {
        return 0;
    } else { // set key to the path that was received
        strcpy(key, msg.msgText);
        return 1;
    }
}

void shuffle(int nMappers, int nReducers) {
    DIR *dir;
    struct dirent *entry;
    key_t key = ftok(".", 5473708);
    struct msgBuffer msg;
    char path[50];
    int msg_id;

    if ((msg_id = msgget(key, 0666 | IPC_CREAT)) == -1) {
        printf("Message get failed.\n");
        exit(1);
    }

    // opening directory for each mapper
    for (int i = 0; i < nMappers; i++) {
        sprintf(path, "output/MapOut/Map_%d", i + 1);
        dir = opendir(path);
        if (dir == NULL) {
            printf("The path passed in is invalid.\n");
            exit(1);
        }

        // read each .txt file in each directory and send path to each reducer
        while ((entry = readdir(dir)) != NULL) {
            //.txt file name
            char *wordFileName = entry->d_name;
            // skips . and .. directory
            if (strcmp(wordFileName, ".") == 0 || strcmp(wordFileName, "..") == 0) {
                continue;
            }

            // creates specific reducerid using hash function
            int reducerid = hashFunction(wordFileName, nReducers);
            memset(msg.msgText, '\0', MSGSIZE);
            memset(path, '\0', 50);
            // get correct path to file and copy into path variable
            sprintf(path, "output/MapOut/Map_%d/%s", i + 1, wordFileName);
            strcpy(msg.msgText, path);
            msg.msgType = reducerid + 1;
            // send to reducer based on reducerid
            if (msgsnd(msg_id, (void *)&msg, sizeof(msg.msgText), 0) == -1) {
                printf("Message send error.\n");
                exit(1);
            }
        }
        closedir(dir);
    }

    // send "END" message to each reducer
    for (int i = 0; i < nReducers; i++) {
        msg.msgType = i + 1;
        memset(msg.msgText, '\0', MSGSIZE);
        strcpy(msg.msgText, "END");
        if (msgsnd(msg_id, (void *)&msg, sizeof(msg.msgText), 0) == -1) {
            printf("Message send error.\n");
            exit(1);
        }
    }
}

// check if the character is valid for a word
int validChar(char c) {
    return (tolower(c) >= 'a' && tolower(c) <= 'z') ||
           (c >= '0' && c <= '9');
}

char *getWord(char *chunk, int *i) {
    char *buffer = (char *)malloc(sizeof(char) * chunkSize);
    memset(buffer, '\0', chunkSize);
    int j = 0;
    while ((*i) < strlen(chunk)) {
        // read a single word at a time from chunk
        // printf("%d\n", i);
        if (chunk[(*i)] == '\n' || chunk[(*i)] == ' ' || !validChar(chunk[(*i)]) || chunk[(*i)] == 0x0) {
            buffer[j] = '\0';
            if (strlen(buffer) > 0) {
                (*i)++;
                return buffer;
            }
            j = 0;
            (*i)++;
            continue;
        }
        buffer[j] = chunk[(*i)];
        j++;
        (*i)++;
    }
    if (strlen(buffer) > 0)
        return buffer;
    return NULL;
}

void createOutputDir() {
    mkdir("output", ACCESSPERMS);
    mkdir("output/MapOut", ACCESSPERMS);
    mkdir("output/ReduceOut", ACCESSPERMS);
}

char *createMapDir(int mapperID) {
    char *dirName = (char *)malloc(sizeof(char) * 100);
    memset(dirName, '\0', 100);
    sprintf(dirName, "output/MapOut/Map_%d", mapperID);
    mkdir(dirName, ACCESSPERMS);
    return dirName;
}

void removeOutputDir() {
    pid_t pid = fork();
    if (pid == 0) {
        char *argv[] = {"rm", "-rf", "output", NULL};
        if (execvp(*argv, argv) < 0) {
            printf("ERROR: exec failed\n");
            exit(1);
        }
        exit(0);
    } else {
        wait(NULL);
    }
}

void bookeepingCode() {
    removeOutputDir();
    sleep(1);
    createOutputDir();
}