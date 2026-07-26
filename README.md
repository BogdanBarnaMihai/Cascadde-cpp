Cascade: Reactive Dataflow Engine

Cascade is a multi-threaded, high-performance C++ framework designed for reactive dataflow processing, node-based graph composition, and real-time audio or mathematical computation.

Features
Node-Component Architecture: Flexible, composition-based design allowing dynamic attachment of functional components (Audio, Transform, Math, and Connection handling) to nodes.

Reactive Dataflow Graph: Type-safe input/output ports managed via a directed dependency graph with automated cycle detection and topological sorting.

Real-Time Performance: Includes low-level utilities like a lock-free ring buffer queue (LockFreeQueue) and a custom memory pool (MemoryPool) to avoid runtime heap overhead.

Undo/Redo Command System: Full transaction history support using the Command pattern for safe modification logging and state rollback.

Prerequisites

A C++ compiler supporting C++17 or later (e.g., g++, clang++).
