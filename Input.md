# Machine configurations: A variety of machine classes with different architectures.

machine class:
{
        Number of machines: 16
        CPU type: X86
        Number of cores: 8
        Memory: 16384
        S-States: [120, 100, 100, 80, 40, 10, 0]
        P-States: [12, 8, 6, 4]
        C-States: [12, 3, 1, 0]
        MIPS: [1000, 800, 600, 400]
        GPUs: yes
}
machine class:
{
        Number of machines: 20
        CPU type: ARM
        Number of cores: 16
        Memory: 32768
        S-States: [130, 110, 90, 70, 50, 20, 0]
        P-States: [14, 10, 7, 5]
        C-States: [12, 4, 2, 0]
        MIPS: [1200, 1000, 750, 500]
        GPUs: no
}
machine class:
{
        Number of machines: 10
        CPU type: POWER
        Number of cores: 32
        Memory: 65536
        S-States: [140, 110, 90, 70, 30, 10, 0]
        P-States: [14, 10, 7, 5]
        C-States: [15, 4, 2, 0]
        MIPS: [1500, 1200, 900, 600]
        GPUs: yes
}
machine class:
{
        Number of machines: 8
        CPU type: RISCV
        Number of cores: 4
        Memory: 8192
        S-States: [100, 80, 60, 40, 20, 10, 0]
        P-States: [10, 7, 5, 3]
        C-States: [10, 3, 1, 0]
        MIPS: [800, 600, 400, 200]
        GPUs: no
}

# Normal workload mix: Various task types, memory usage, and SLA constraints.

task class:
{
        Start time: 50000
        End time : 500000
        Inter arrival: 10000
        Expected runtime: 2500000
        Memory: 8
        VM type: LINUX
        GPU enabled: no
        SLA type: SLA0
        CPU type: X86
        Task type: WEB
        Seed: 100001
}
task class:
{
        Start time: 120000
        End time : 800000
        Inter arrival: 15000
        Expected runtime: 4000000
        Memory: 16
        VM type: WIN
        GPU enabled: no
        SLA type: SLA1
        CPU type: ARM
        Task type: STREAM
        Seed: 100002
}
task class:
{
        Start time: 200000
        End time : 900000
        Inter arrival: 20000
        Expected runtime: 5000000
        Memory: 32
        VM type: LINUX_RT
        GPU enabled: yes
        SLA type: SLA2
        CPU type: POWER
        Task type: AI
        Seed: 100003
}
task class:
{
        Start time: 300000
        End time : 700000
        Inter arrival: 8000
        Expected runtime: 3000000
        Memory: 12
        VM type: WIN
        GPU enabled: no
        SLA type: SLA3
        CPU type: RISCV
        Task type: CRYPTO
        Seed: 100004
}

# Load spike: Simulate a temporary high-load scenario at 600000 microseconds.

task class:
{
        Start time: 600000
        End time : 650000
        Inter arrival: 5000
        Expected runtime: 5000000
        Memory: 24
        VM type: LINUX
        GPU enabled: yes
        SLA type: SLA0
        CPU type: X86
        Task type: AI
        Seed: 200001
}
task class:
{
        Start time: 600000
        End time : 650000
        Inter arrival: 4000
        Expected runtime: 3500000
        Memory: 10
        VM type: LINUX
        GPU enabled: no
        SLA type: SLA0
        CPU type: X86
        Task type: WEB
        Seed: 200002
}

# Debugging tasks: These tasks have shorter simulation times and can be extracted for quick testing.

task class:
{
        Start time: 10000
        End time : 50000
        Inter arrival: 2000
        Expected runtime: 500000
        Memory: 4
        VM type: LINUX
        GPU enabled: no
        SLA type: SLA3
        CPU type: ARM
        Task type: STREAM
        Seed: 300001
}
task class:
{
        Start time: 15000
        End time : 60000
        Inter arrival: 3000
        Expected runtime: 600000
        Memory: 6
        VM type: WIN
        GPU enabled: no
        SLA type: SLA2
        CPU type: RISCV
        Task type: CRYPTO
        Seed: 300002
}
