echo "--------------kotlin--------------" && cd ./kotlin && kotlinc Storage.kt rjson.kt TestStorage.kt -include-runtime -d storage-tests.jar && java -jar storage-tests.jar  && cd ..
echo "---------------rust---------------" && cd ./rust && cargo run && cd ..
echo "--------------- ts ---------------" && cd ./ts && npx tsx TestStorage.ts && cd ..
echo "--------------- js ---------------" && cd ./js && node TestStorage.js && cd ..
echo "--------------- py ---------------" && python3 ./SingletonKeyValueStorage/Storages/TestStorage.py
echo "---------------cpp ---------------" && cd ./cpp/build && cmake .. && cmake --build . --config Release && ./SingletonStorage && cd .. && cd ..
