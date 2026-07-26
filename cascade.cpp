#include <iostream>
#include <memory>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <functional>
#include <algorithm>
#include <queue>
#include <stack>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <chrono>
#include <thread>
#include <variant>
#include <cmath>
#include <sstream>
#include <cassert>
#include <optional>

class Node;
class Engine;
class Component;
class Port;
class DependencyGraph;
class Command;
class CommandHistory;

using Variant = std::variant<
    std::monostate,
    bool,
    int,
    float,
    double,
    std::string,
    std::vector<float>,
    std::vector<double>
>;

template <typename T, size_t Capacity = 1024>
class LockFreeQueue {
    static_assert((Capacity & (Capacity - 1)) == 0);
    
    std::vector<T> m_buffer{Capacity};
    std::atomic<size_t> m_readIndex{0};
    std::atomic<size_t> m_writeIndex{0};
    char m_padding1[64 - sizeof(std::atomic<size_t>)];
    char m_padding2[64 - sizeof(std::atomic<size_t>)];

public:
    bool tryPush(const T& item) {
        size_t write = m_writeIndex.load(std::memory_order_relaxed);
        size_t next = (write + 1) % Capacity;
        
        if (next == m_readIndex.load(std::memory_order_acquire))
            return false;
            
        m_buffer[write] = item;
        m_writeIndex.store(next, std::memory_order_release);
        return true;
    }

    bool tryPop(T& item) {
        size_t read = m_readIndex.load(std::memory_order_relaxed);
        
        if (read == m_writeIndex.load(std::memory_order_acquire))
            return false;
            
        item = std::move(m_buffer[read]);
        m_readIndex.store((read + 1) % Capacity, std::memory_order_release);
        return true;
    }

    bool isEmpty() const {
        return m_readIndex.load(std::memory_order_acquire) == m_writeIndex.load(std::memory_order_acquire);
    }
};

template <typename T, size_t BlockSize = 256>
class MemoryPool {
    struct Block {
        std::vector<T> objects;
        Block() : objects(BlockSize) {}
    };

    std::vector<std::unique_ptr<Block>> m_blocks;
    std::stack<T*> m_freeList;
    mutable std::mutex m_mutex;

    void allocateBlock() {
        auto block = std::make_unique<Block>();
        for (size_t i = 0; i < BlockSize; i++) {
            m_freeList.push(&block->objects[i]);
        }
        m_blocks.push_back(std::move(block));
    }

public:
    T* acquire() {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_freeList.empty()) {
            allocateBlock();
        }
        T* obj = m_freeList.top();
        m_freeList.pop();
        return obj;
    }

    void release(T* obj) {
        if (!obj) return;
        *obj = T{};
        std::lock_guard<std::mutex> lock(m_mutex);
        m_freeList.push(obj);
    }

    size_t available() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_freeList.size();
    }
};

template <typename T>
class Property {
    std::string m_name;
    T m_value;
    std::atomic<bool> m_dirty{false};
    std::vector<std::function<void(const T&)>> m_observers;
    mutable std::shared_mutex m_mutex;

public:
    Property(const std::string& name, T initial)
        : m_name(name), m_value(initial) {}

    void setValue(const T& val) {
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        if (m_value != val) {
            m_value = val;
            m_dirty = true;
            auto observers = m_observers;
            lock.unlock();
            
            for (auto& cb : observers) {
                cb(m_value);
            }
        }
    }

    T getValue() const {
        std::shared_lock<std::shared_mutex> lock(m_mutex);
        return m_value;
    }

    const std::string& getName() const { return m_name; }
    bool isDirty() const { return m_dirty.load(); }
    void markClean() { m_dirty = false; }

    void observe(std::function<void(const T&)> cb) {
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        m_observers.push_back(cb);
    }
};

enum class PortDirection { Input, Output };
enum class PortDataType { Float, Double, Int, String, Audio, Image, Custom };

class Port {
public:
    using PortId = uint64_t;

    Port(PortId id, const std::string& name, PortDirection dir, PortDataType type)
        : m_id(id), m_name(name), m_direction(dir), m_dataType(type) {}

    PortId getId() const { return m_id; }
    const std::string& getName() const { return m_name; }
    PortDirection getDirection() const { return m_direction; }
    PortDataType getDataType() const { return m_dataType; }

    void setValue(const Variant& val) {
        std::unique_lock<std::shared_mutex> lock(m_valueMutex);
        m_value = val;
    }

    Variant getValue() const {
        std::shared_lock<std::shared_mutex> lock(m_valueMutex);
        return m_value;
    }

private:
    PortId m_id;
    std::string m_name;
    PortDirection m_direction;
    PortDataType m_dataType;
    Variant m_value;
    mutable std::shared_mutex m_valueMutex;
};

class DependencyGraph {
public:
    using PortId = uint64_t;

    void addConnection(PortId source, PortId dest) {
        auto& vec = m_adjacency[source];
        if (std::find(vec.begin(), vec.end(), dest) != vec.end())
            return;
            
        if (wouldCreateCycle(source, dest))
            throw std::runtime_error("Connection would create a cycle");
            
        m_adjacency[source].push_back(dest);
        m_reverseAdjacency[dest].push_back(source);
        m_inDegree[dest]++;
        
        if (m_inDegree.find(source) == m_inDegree.end())
            m_inDegree[source] = 0;
    }

    bool removeConnection(PortId source, PortId dest) {
        auto it = m_adjacency.find(source);
        if (it == m_adjacency.end()) return false;
        
        auto& vec = it->second;
        auto found = std::find(vec.begin(), vec.end(), dest);
        if (found == vec.end()) return false;
        
        vec.erase(found);
        
        auto& rev = m_reverseAdjacency[dest];
        rev.erase(std::remove(rev.begin(), rev.end(), source), rev.end());
        
        m_inDegree[dest]--;
        return true;
    }

    bool wouldCreateCycle(PortId source, PortId dest) const {
        if (source == dest) return true;
        
        std::unordered_set<PortId> visited;
        std::vector<PortId> stack;
        
        auto it = m_adjacency.find(dest);
        if (it != m_adjacency.end()) {
            for (PortId neighbor : it->second) {
                stack.push_back(neighbor);
            }
        }
        
        while (!stack.empty()) {
            PortId current = stack.back();
            stack.pop_back();
            
            if (current == source) return true;
            if (visited.count(current)) continue;
            
            visited.insert(current);
            auto adjIt = m_adjacency.find(current);
            
            if (adjIt != m_adjacency.end()) {
                for (PortId neighbor : adjIt->second) {
                    if (!visited.count(neighbor)) {
                        stack.push_back(neighbor);
                    }
                }
            }
        }
        return false;
    }

    std::vector<PortId> topologicalSort(const std::unordered_set<PortId>& dirtyPorts) const {
        std::unordered_map<PortId, int> tempInDegree;
        std::unordered_set<PortId> affected;
        std::queue<PortId> queue;
        
        for (PortId p : dirtyPorts) {
            queue.push(p);
        }
        
        while (!queue.empty()) {
            PortId current = queue.front();
            queue.pop();
            
            if (affected.count(current)) continue;
            affected.insert(current);
            
            auto it = m_adjacency.find(current);
            if (it != m_adjacency.end()) {
                for (PortId next : it->second) {
                    if (!affected.count(next)) {
                        queue.push(next);
                    }
                }
            }
        }
        
        for (PortId p : affected) {
            tempInDegree[p] = 0;
            auto it = m_reverseAdjacency.find(p);
            if (it != m_reverseAdjacency.end()) {
                for (PortId src : it->second) {
                    if (affected.count(src)) {
                        tempInDegree[p]++;
                    }
                }
            }
        }
        
        std::queue<PortId> zeroDegree;
        for (auto& [port, degree] : tempInDegree) {
            if (degree == 0) {
                zeroDegree.push(port);
            }
        }
        
        std::vector<PortId> result;
        while (!zeroDegree.empty()) {
            PortId current = zeroDegree.front();
            zeroDegree.pop();
            result.push_back(current);
            
            auto it = m_adjacency.find(current);
            if (it != m_adjacency.end()) {
                for (PortId next : it->second) {
                    if (affected.count(next) && --tempInDegree[next] == 0) {
                        zeroDegree.push(next);
                    }
                }
            }
        }
        return result;
    }

    void clear() {
        m_adjacency.clear();
        m_reverseAdjacency.clear();
        m_inDegree.clear();
    }

    size_t connectionCount() const {
        size_t count = 0;
        for (auto& [_, vec] : m_adjacency) {
            count += vec.size();
        }
        return count;
    }

    void print() const {
        std::cout << "Dependency Graph (" << connectionCount() << " connections):\n";
        for (auto& [source, dests] : m_adjacency) {
            for (auto dest : dests) {
                std::cout << " Port " << source << " -> Port " << dest << "\n";
            }
        }
    }

private:
    std::unordered_map<PortId, std::vector<PortId>> m_adjacency;
    std::unordered_map<PortId, std::vector<PortId>> m_reverseAdjacency;
    std::unordered_map<PortId, int> m_inDegree;
};

class Component {
public:
    virtual ~Component() = default;
    virtual void process() = 0;
    virtual std::string getTypeName() const = 0;
    virtual std::unordered_map<std::string, Variant> serialize() const { return {}; }
    virtual void deserialize(const std::unordered_map<std::string, Variant>&) {}

    void setOwner(Node* node) { m_owner = node; }
    Node* getOwner() const { return m_owner; }

protected:
    Node* m_owner = nullptr;
};

class TransformComponent : public Component {
    float m_x = 0, m_y = 0;
    float m_rotation = 0;
    float m_scaleX = 1, m_scaleY = 1;

public:
    TransformComponent() = default;
    TransformComponent(float x, float y) : m_x(x), m_y(y) {}

    void process() override {}
    std::string getTypeName() const override { return "Transform"; }

    void setPosition(float x, float y) { m_x = x; m_y = y; }
    float getX() const { return m_x; }
    float getY() const { return m_y; }

    void setRotation(float r) { m_rotation = r; }
    float getRotation() const { return m_rotation; }

    void setScale(float sx, float sy) { m_scaleX = sx; m_scaleY = sy; }
    float getScaleX() const { return m_scaleX; }
    float getScaleY() const { return m_scaleY; }

    std::unordered_map<std::string, Variant> serialize() const override {
        return {
            {"x", m_x},
            {"y", m_y},
            {"rotation", m_rotation},
            {"scaleX", m_scaleX},
            {"scaleY", m_scaleY}
        };
    }
};

class AudioComponent : public Component {
    std::vector<float> m_buffer;
    int m_numChannels = 2;
    int m_bufferSize = 512;
    double m_sampleRate = 44100.0;
    float m_volume = 1.0f;
    float m_pan = 0.0f;
    float m_phase = 0.0f;
    float m_frequency = 440.0f;

public:
    AudioComponent() {
        m_buffer.resize(m_numChannels * m_bufferSize, 0.0f);
    }

    void process() override {
        generateSineTone(m_frequency);
        applyVolume();
        applyPanning();
    }

    std::string getTypeName() const override { return "Audio"; }

    void setFrequency(float freq) { m_frequency = freq; }
    float getFrequency() const { return m_frequency; }

    void setVolume(float vol) { m_volume = std::clamp(vol, 0.0f, 2.0f); }
    float getVolume() const { return m_volume; }

    void setPan(float p) { m_pan = std::clamp(p, -1.0f, 1.0f); }
    const float* getBuffer() const { return m_buffer.data(); }
    int getBufferSize() const { return m_bufferSize; }
    int getNumChannels() const { return m_numChannels; }

    std::unordered_map<std::string, Variant> serialize() const override {
        return {
            {"frequency", m_frequency},
            {"volume", m_volume},
            {"pan", m_pan},
            {"numChannels", m_numChannels}
        };
    }

private:
    void generateSineTone(float freq) {
        float increment = 2.0f * M_PI * freq / m_sampleRate;
        for (int i = 0; i < m_bufferSize; i++) {
            float sample = std::sin(m_phase);
            for (int ch = 0; ch < m_numChannels; ch++) {
                m_buffer[i * m_numChannels + ch] = sample;
            }
            m_phase += increment;
            if (m_phase >= 2.0f * M_PI) {
                m_phase -= 2.0f * M_PI;
            }
        }
    }

    void applyVolume() {
        for (auto& sample : m_buffer) {
            sample *= m_volume;
        }
    }

    void applyPanning() {
        if (m_numChannels < 2) return;
        float leftGain = (m_pan <= 0) ? 1.0f : (1.0f - m_pan);
        float rightGain = (m_pan >= 0) ? 1.0f : (1.0f + m_pan);
        
        for (int i = 0; i < m_bufferSize; i++) {
            m_buffer[i * m_numChannels] *= leftGain;
            m_buffer[i * m_numChannels + 1] *= rightGain;
        }
    }
};

class ConnectionComponent : public Component {
    std::vector<std::shared_ptr<Port>> m_inputPorts;
    std::vector<std::shared_ptr<Port>> m_outputPorts;
    Port::PortId m_nextPortId;

public:
    ConnectionComponent() : m_nextPortId(1) {}
    ConnectionComponent(Port::PortId startId) : m_nextPortId(startId) {}

    void process() override {}
    std::string getTypeName() const override { return "Connection"; }

    std::shared_ptr<Port> addInputPort(const std::string& name, PortDataType type) {
        auto port = std::make_shared<Port>(m_nextPortId++, name, PortDirection::Input, type);
        m_inputPorts.push_back(port);
        return port;
    }

    std::shared_ptr<Port> addOutputPort(const std::string& name, PortDataType type) {
        auto port = std::make_shared<Port>(m_nextPortId++, name, PortDirection::Output, type);
        m_outputPorts.push_back(port);
        return port;
    }

    std::shared_ptr<Port> getInputPort(const std::string& name) const {
        for (auto& p : m_inputPorts) {
            if (p->getName() == name) return p;
        }
        return nullptr;
    }

    std::shared_ptr<Port> getOutputPort(const std::string& name) const {
        for (auto& p : m_outputPorts) {
            if (p->getName() == name) return p;
        }
        return nullptr;
    }

    const auto& getInputPorts() const { return m_inputPorts; }
    const auto& getOutputPorts() const { return m_outputPorts; }

    std::unordered_map<std::string, Variant> serialize() const override {
        return {
            {"inputCount", (int)m_inputPorts.size()},
            {"outputCount", (int)m_outputPorts.size()}
        };
    }
};

class ProcessingStrategy {
public:
    virtual ~ProcessingStrategy() = default;
    virtual double compute(const std::vector<double>& inputs) = 0;
    virtual std::string getName() const = 0;
};

class AdditionStrategy : public ProcessingStrategy {
public:
    double compute(const std::vector<double>& inputs) override {
        double sum = 0;
        for (double v : inputs) sum += v;
        return sum;
    }
    std::string getName() const override { return "Addition"; }
};

class MultiplicationStrategy : public ProcessingStrategy {
public:
    double compute(const std::vector<double>& inputs) override {
        double product = 1;
        for (double v : inputs) product *= v;
        return product;
    }
    std::string getName() const override { return "Multiplication"; }
};

class AverageStrategy : public ProcessingStrategy {
public:
    double compute(const std::vector<double>& inputs) override {
        if (inputs.empty()) return 0;
        double sum = 0;
        for (double v : inputs) sum += v;
        return sum / inputs.size();
    }
    std::string getName() const override { return "Average"; }
};

class MathComponent : public Component {
    std::unique_ptr<ProcessingStrategy> m_strategy;
    std::vector<double> m_inputs;
    double m_output = 0.0;
    bool m_enabled = true;

public:
    MathComponent() : m_strategy(std::make_unique<AdditionStrategy>()) {}

    void process() override {
        if (m_enabled && m_strategy) {
            m_output = m_strategy->compute(m_inputs);
        }
    }

    std::string getTypeName() const override { return "Math"; }

    void setStrategy(const std::string& name) {
        if (name == "Addition") m_strategy = std::make_unique<AdditionStrategy>();
        else if (name == "Multiplication") m_strategy = std::make_unique<MultiplicationStrategy>();
        else if (name == "Average") m_strategy = std::make_unique<AverageStrategy>();
    }

    std::string getStrategyName() const {
        return m_strategy ? m_strategy->getName() : "None";
    }

    void setInputs(const std::vector<double>& inputs) { m_inputs = inputs; }
    void addInput(double val) { m_inputs.push_back(val); }
    double getOutput() const { return m_output; }
    void setEnabled(bool e) { m_enabled = e; }

    std::unordered_map<std::string, Variant> serialize() const override {
        return {
            {"strategy", getStrategyName()},
            {"output", m_output},
            {"enabled", m_enabled}
        };
    }
};

class Node {
public:
    using NodeId = uint64_t;

    Node(NodeId id, Engine* engine) : m_id(id), m_engine(engine) {}

    NodeId getId() const { return m_id; }
    Engine* getEngine() const { return m_engine; }

    template <typename T, typename... Args>
    T* addComponent(Args&&... args) {
        auto comp = std::make_unique<T>(std::forward<Args>(args)...);
        comp->setOwner(this);
        T* ptr = comp.get();
        std::string typeName = comp->getTypeName();
        m_components[typeName] = std::move(comp);
        return ptr;
    }

    template <typename T>
    T* getComponent() {
        auto it = m_components.find(T().getTypeName());
        if (it != m_components.end())
            return static_cast<T*>(it->second.get());
        return nullptr;
    }

    template <typename T>
    std::shared_ptr<Property<T>> addProperty(const std::string& name, T initial) {
        auto prop = std::make_shared<Property<T>>(name, initial);
        m_properties.push_back(prop);
        return prop;
    }

    void process() {
        for (auto& [name, comp] : m_components) {
            comp->process();
        }
    }

    void print() const {
        std::cout << "Node " << m_id << ":\n";
        for (auto& [name, comp] : m_components) {
            std::cout << " Component: " << name << "\n";
        }
    }

    std::string serialize() const {
        std::stringstream ss;
        ss << "Node{id:" << m_id << ",components:[";
        bool first = true;
        for (auto& [name, comp] : m_components) {
            if (!first) ss << ",";
            ss << name;
            first = false;
        }
        ss << "]}";
        return ss.str();
    }

private:
    NodeId m_id;
    Engine* m_engine;
    std::unordered_map<std::string, std::unique_ptr<Component>> m_components;
    std::vector<std::shared_ptr<void>> m_properties;
};

class Command {
public:
    virtual ~Command() = default;
    virtual void execute() = 0;
    virtual void undo() = 0;
    virtual std::string getDescription() const = 0;
};

class CommandHistory {
    std::vector<std::unique_ptr<Command>> m_undoStack;
    std::vector<std::unique_ptr<Command>> m_redoStack;
    size_t m_maxHistory = 100;

public:
    void executeCommand(std::unique_ptr<Command> cmd) {
        cmd->execute();
        m_redoStack.clear();
        m_undoStack.push_back(std::move(cmd));
        if (m_undoStack.size() > m_maxHistory) {
            m_undoStack.erase(m_undoStack.begin());
        }
    }

    bool undo() {
        if (m_undoStack.empty()) return false;
        auto cmd = std::move(m_undoStack.back());
        m_undoStack.pop_back();
        cmd->undo();
        m_redoStack.push_back(std::move(cmd));
        return true;
    }

    bool redo() {
        if (m_redoStack.empty()) return false;
        auto cmd = std::move(m_redoStack.back());
        m_redoStack.pop_back();
        cmd->execute();
        m_undoStack.push_back(std::move(cmd));
        return true;
    }

    bool canUndo() const { return !m_undoStack.empty(); }
    bool canRedo() const { return !m_redoStack.empty(); }
    size_t getUndoCount() const { return m_undoStack.size(); }
    size_t getRedoCount() const { return m_redoStack.size(); }
};

class Engine {
public:
    using NodeId = Node::NodeId;
    using PortId = Port::PortId;

    Engine() 
        : m_depGraph(std::make_unique<DependencyGraph>()),
          m_cmdHistory(std::make_unique<CommandHistory>()) {
        registerDefaultTypes();
    }

    std::shared_ptr<Node> createNode(const std::string& type, NodeId customId = 0) {
        std::unique_lock<std::shared_mutex> lock(m_nodeMutex);
        NodeId id = customId ? customId : m_nextNodeId++;
        auto it = m_factories.find(type);
        if (it == m_factories.end())
            throw std::runtime_error("Unknown node type: " + type);
            
        auto node = it->second(id, this);
        m_nodes[id] = node;
        return node;
    }

    bool removeNode(NodeId id) {
        std::unique_lock<std::shared_mutex> lock(m_nodeMutex);
        auto it = m_nodes.find(id);
        if (it == m_nodes.end()) return false;
        m_nodes.erase(it);
        return true;
    }

    std::shared_ptr<Node> getNode(NodeId id) const {
        std::shared_lock<std::shared_mutex> lock(m_nodeMutex);
        auto it = m_nodes.find(id);
        return it != m_nodes.end() ? it->second : nullptr;
    }

    void connect(PortId source, PortId dest) {
        m_depGraph->addConnection(source, dest);
    }

    void disconnect(PortId source, PortId dest) {
        m_depGraph->removeConnection(source, dest);
    }

    void process() {
        std::shared_lock<std::shared_mutex> lock(m_nodeMutex);
        for (auto& [id, node] : m_nodes) {
            node->process();
        }
    }

    void executeCommand(std::unique_ptr<Command> cmd) {
        m_cmdHistory->executeCommand(std::move(cmd));
    }

    bool undo() { return m_cmdHistory->undo(); }
    bool redo() { return m_cmdHistory->redo(); }
    bool canUndo() const { return m_cmdHistory->canUndo(); }
    bool canRedo() const { return m_cmdHistory->canRedo(); }

    void registerNodeType(const std::string& type,
                          std::function<std::shared_ptr<Node>(NodeId, Engine*)> factory) {
        m_factories[type] = factory;
    }

    PortId allocatePortId() {
        return m_nextPortId++;
    }

    void printStats() const {
        std::shared_lock<std::shared_mutex> lock(m_nodeMutex);
        std::cout << "\n=== Engine Statistics ===\n";
        std::cout << "Nodes: " << m_nodes.size() << "\n";
        std::cout << "Connections: " << m_depGraph->connectionCount() << "\n";
        std::cout << "Undo stack: " << m_cmdHistory->getUndoCount() << "\n";
        std::cout << "Redo stack: " << m_cmdHistory->getRedoCount() << "\n";
        for (auto& [id, node] : m_nodes) {
            node->print();
        }
    }

    void printGraph() const {
        m_depGraph->print();
    }

    const DependencyGraph& getGraph() const { return *m_depGraph; }

private:
    std::unique_ptr<DependencyGraph> m_depGraph;
    std::unique_ptr<CommandHistory> m_cmdHistory;
    std::unordered_map<NodeId, std::shared_ptr<Node>> m_nodes;
    NodeId m_nextNodeId = 1;
    std::atomic<PortId> m_nextPortId{1};
    mutable std::shared_mutex m_nodeMutex;
    std::unordered_map<std::string, std::function<std::shared_ptr<Node>(NodeId, Engine*)>> m_factories;

    void registerDefaultTypes() {
        registerNodeType("Generic", [](NodeId id, Engine* eng) {
            auto node = std::make_shared<Node>(id, eng);
            node->addComponent<ConnectionComponent>(eng->allocatePortId());
            node->addComponent<TransformComponent>();
            return node;
        });

        registerNodeType("AudioSource", [](NodeId id, Engine* eng) {
            auto node = std::make_shared<Node>(id, eng);
            auto* conn = node->addComponent<ConnectionComponent>(eng->allocatePortId());
            conn->addOutputPort("audio_out", PortDataType::Audio);
            auto* audio = node->addComponent<AudioComponent>();
            audio->setFrequency(440.0f);
            audio->setVolume(0.8f);
            node->addComponent<TransformComponent>(100.0f, 100.0f);
            return node;
        });

        registerNodeType("AudioOutput", [](NodeId id, Engine* eng) {
            auto node = std::make_shared<Node>(id, eng);
            auto* conn = node->addComponent<ConnectionComponent>(eng->allocatePortId());
            conn->addInputPort("audio_in", PortDataType::Audio);
            node->addComponent<TransformComponent>(400.0f, 100.0f);
            return node;
        });

        registerNodeType("Math", [](NodeId id, Engine* eng) {
            auto node = std::make_shared<Node>(id, eng);
            auto* conn = node->addComponent<ConnectionComponent>(eng->allocatePortId());
            conn->addInputPort("input_a", PortDataType::Float);
            conn->addInputPort("input_b", PortDataType::Float);
            conn->addOutputPort("result", PortDataType::Float);
            auto* math = node->addComponent<MathComponent>();
            math->setStrategy("Addition");
            node->addComponent<TransformComponent>(250.0f, 200.0f);
            return node;
        });

        registerNodeType("Mixer", [](NodeId id, Engine* eng) {
            auto node = std::make_shared<Node>(id, eng);
            auto* conn = node->addComponent<ConnectionComponent>(eng->allocatePortId());
            conn->addInputPort("ch1", PortDataType::Audio);
            conn->addInputPort("ch2", PortDataType::Audio);
            conn->addOutputPort("mix_out", PortDataType::Audio);
            node->addComponent<TransformComponent>(250.0f, 300.0f);
            return node;
        });
    }
};

class AddNodeCommand : public Command {
    Engine* m_engine;
    std::string m_nodeType;
    Node::NodeId m_nodeId;
    bool m_executed = false;

public:
    AddNodeCommand(Engine* engine, const std::string& type, Node::NodeId id)
        : m_engine(engine), m_nodeType(type), m_nodeId(id) {}

    void execute() override {
        if (!m_executed) {
            m_engine->createNode(m_nodeType, m_nodeId);
            m_executed = true;
        }
    }

    void undo() override {
        if (m_executed) {
            m_engine->removeNode(m_nodeId);
            m_executed = false;
        }
    }

    std::string getDescription() const override {
        return "Add " + m_nodeType + " node (ID: " + std::to_string(m_nodeId) + ")";
    }
};

class RemoveNodeCommand : public Command {
    Engine* m_engine;
    Node::NodeId m_nodeId;
    std::string m_serializedNode;
    bool m_executed = false;

public:
    RemoveNodeCommand(Engine* engine, Node::NodeId id)
        : m_engine(engine), m_nodeId(id) {}

    void execute() override {
        if (!m_executed) {
            auto node = m_engine->getNode(m_nodeId);
            if (node) {
                m_serializedNode = node->serialize();
                m_engine->removeNode(m_nodeId);
                m_executed = true;
            }
        }
    }

    void undo() override {
        if (m_executed) {
            m_executed = false;
        }
    }

    std::string getDescription() const override {
        return "Remove node (ID: " + std::to_string(m_nodeId) + ")";
    }
};

void demonstrateBasicGraph() {
    std::cout << "\n========================================\n";
    std::cout << " Cascade - Reactive Dataflow Engine\n";
    std::cout << "========================================\n";

    Engine engine;

    std::cout << "\n[1] Creating nodes...\n";
    auto osc1 = engine.createNode("AudioSource");
    auto osc2 = engine.createNode("AudioSource");
    auto mixer = engine.createNode("Mixer");
    auto output = engine.createNode("AudioOutput");
    std::cout << " Created: AudioSource (x2), Mixer, AudioOutput\n";

    auto* audio1 = osc1->getComponent<AudioComponent>();
    auto* audio2 = osc2->getComponent<AudioComponent>();

    audio1->setFrequency(261.63f);
    audio1->setVolume(0.7f);
    audio2->setFrequency(329.63f);
    audio2->setVolume(0.5f);

    std::cout << " OSC1: Frequency=261.63 Hz (C4), Volume=0.7\n";
    std::cout << " OSC2: Frequency=329.63 Hz (E4), Volume=0.5\n";

    auto* conn1 = osc1->getComponent<ConnectionComponent>();
    auto* conn2 = osc2->getComponent<ConnectionComponent>();
    auto* connMix = mixer->getComponent<ConnectionComponent>();
    auto* connOut = output->getComponent<ConnectionComponent>();

    std::cout << "\n[2] Connecting graph...\n";
    auto osc1Out = conn1->getOutputPort("audio_out");
    auto osc2Out = conn2->getOutputPort("audio_out");
    auto mixCh1 = connMix->getInputPort("ch1");
    auto mixCh2 = connMix->getInputPort("ch2");
    auto mixOut = connMix->getOutputPort("mix_out");
    auto outIn = connOut->getInputPort("audio_in");

    try {
        engine.connect(osc1Out->getId(), mixCh1->getId());
        std::cout << " Connected: OSC1 -> Mixer[ch1]\n";
        
        engine.connect(osc2Out->getId(), mixCh2->getId());
        std::cout << " Connected: OSC2 -> Mixer[ch2]\n";
        
        engine.connect(mixOut->getId(), outIn->getId());
        std::cout << " Connected: Mixer -> Output\n";
    } catch (const std::exception& e) {
        std::cout << " Error: " << e.what() << "\n";
    }

    std::cout << "\n[3] Processing graph (simulating audio)...\n";
    engine.process();

    const float* buffer = audio1->getBuffer();
    std::cout << " OSC1 first 10 samples: ";
    for (int i = 0; i < 10; i++) {
        printf("%.3f ", buffer[i]);
    }
    printf("\n");

    std::cout << "\n[4] Modifying properties in real-time...\n";
    audio1->setFrequency(440.0f);
    audio1->setVolume(1.0f);
    std::cout << " Changed OSC1: Frequency=440 Hz, Volume=1.0\n";

    engine.process();
    printf(" OSC1 new samples: ");
    for (int i = 0; i < 10; i++) {
        printf("%.3f ", buffer[i]);
    }
    printf("\n");

    engine.printStats();
    engine.printGraph();
}

void demonstrateUndoRedo() {
    std::cout << "\n========================================\n";
    std::cout << " Undo/Redo Demonstration\n";
    std::cout << "========================================\n";

    Engine engine;

    std::cout << "\n[1] Creating and connecting nodes via commands...\n";
    auto cmd1 = std::make_unique<AddNodeCommand>(&engine, "Math", 100);
    auto cmd2 = std::make_unique<AddNodeCommand>(&engine, "Math", 101);
    auto cmd3 = std::make_unique<AddNodeCommand>(&engine, "AudioSource", 102);

    std::cout << " Executing: " << cmd1->getDescription() << "\n";
    engine.executeCommand(std::move(cmd1));

    std::cout << " Executing: " << cmd2->getDescription() << "\n";
    engine.executeCommand(std::move(cmd2));

    std::cout << " Executing: " << cmd3->getDescription() << "\n";
    engine.executeCommand(std::move(cmd3));

    engine.printStats();

    std::cout << "\n[2] Performing Undo operations...\n";
    if (engine.canUndo()) {
        std::cout << " Undoing last command...\n";
        engine.undo();
    }
    if (engine.canUndo()) {
        std::cout << " Undoing previous command...\n";
        engine.undo();
    }

    engine.printStats();

    std::cout << "\n[3] Performing Redo operations...\n";
    if (engine.canRedo()) {
        std::cout << " Redoing command...\n";
        engine.redo();
    }

    engine.printStats();
}

int main() {
    demonstrateBasicGraph();
    demonstrateUndoRedo();
    return 0;
}