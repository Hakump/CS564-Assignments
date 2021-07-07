//
// File: wl.cpp
//
//  Description: the HW1 main class file
//  UW Campus ID: 9077246503
//  email: hao45@wisc.edu

#include <iostream>
#include <string>
#include <unordered_map>
#include <list>
#include <vector>
#include "wl.h"
#include <regex>
#include <cctype>
#include <fstream>

using namespace std;
unordered_map<string, vector<int>> wordLists;
std::regex delim("[^a-zA-Z0-9']+");
std::regex space("\\s+");
int num;
/**
 * @param x is the split string check "" here
 * insert the specific string (lowercase) and its position to the wordlist
 */
void insert(string x){
    if (x == "")
        return;
    transform(x.begin(), x.end(), x.begin(), ::tolower);

    auto loc = wordLists.find(x);
    if (loc == wordLists.end()){
        vector<int> l = {++num};
        //insert infact copy the...
        wordLists.insert(make_pair(x,l));
    } else {
        wordLists.at(x).push_back(++num);
    }
}

/**
 * split a string and insert it to wordlist
 * @param str is the stirng required to be splited
 * do not check "" here
 *
 */
void split(string str){
    std::sregex_token_iterator spl(str.begin(), str.end(), delim,-1);
    std::sregex_token_iterator stop;
    for (; spl != stop ; spl++) {
        insert(*spl);
    }
}

int main()
{
    bool isopening = false;  // if a file is loaded
    num = 0; // word count
    string temp; // temp string for getting the input file lines
    cout << ">";
    // keep receiving the input command
    while (getline(cin,temp)) {
        vector<string> argv;
        int argc = 0;
        std::sregex_token_iterator sp(temp.begin(), temp.end(), space,-1);
        std::sregex_token_iterator st;
        for (; sp != st ; sp++) {
            if(*sp == "") continue;
            argv.push_back(*sp);
            argc++;
        }
        if (argc == 0) {
            cout << ">";
            continue;
        }
        transform(argv[0].begin(), argv[0].end(), argv[0].begin(), ::tolower);
        // checking the validity and operate
        if (argc == 1) {
            // check if string
            if (argv[0] == "new") {
                //clean every thing
                num = 0;
                wordLists.clear();
                isopening = false;
                //TODO: do I need to clear each list?
            } else if (argv[0] == "end") {
                // finish everything
                wordLists.clear();
                //TODO: do I need to clear each list?
                exit(0);
            } else {
                cout << "ERROR: Invalid command\n>";
                continue;
            }
        } else if (argc == 2) {
            if (argv[0] != "load") {
                cout << "ERROR: Invalid command\n>";
                continue;
            }

            ifstream inputfile(argv[1]);
            if (!inputfile) { // failed
                cout << "ERROR: Invalid command\n>";
                continue;
            }
            if (isopening) {
                num = 0;
                wordLists.clear();
            }
            isopening = true;
            for (string temp; getline(inputfile, temp);) {
                split(temp);
            }
            inputfile.close();
            //cout << endl;
        } else if (argc == 3 ) {
            if (argv[0] != "locate") { // !isopening ||
                cout << "ERROR: Invalid command\n>";
                continue;
            }
            bool ok = true;
            for (int i = 0; i < (int)argv[2].size(); i++) {
                if (!isdigit(argv[2][i])) {
                    ok = false;
                    cout << "ERROR: Invalid command" <<argv[2][i]<< endl;
                    break;
                }
            }
            // third arg is number and greater than 0
            if (!ok || stoi(argv[2]) < 1) {
                cout << "ERROR: Invalid command\n>";
                continue;
            }
            for (int i = 0; i < (int)argv[1].size(); i++) {
                argv[1][i] = tolower(argv[1][i]);
            }
            auto loc = wordLists.find(argv[1]);
            if (loc == wordLists.end()) {
                cout << "No matching entry\n>";
                continue;
            } else {
                if ((int)(*loc).second.size() < stoi(argv[2])) {
                    cout << "No matching entry\n>";
                    continue;
                } else {
                    cout << (*loc).second.at(stoi(argv[2]) - 1) << endl;
                }
            }
        }
        cout << ">";
    }
    return 0;
}
