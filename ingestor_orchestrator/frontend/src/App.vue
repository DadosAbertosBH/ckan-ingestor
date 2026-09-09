<template>
    <div class="app">
        <nav class="navbar">
            <div class="nav-brand">
                <router-link to="/" class="brand-link">
                    <span class="brand-icon">⚡</span>
                    <span class="brand-text">CKAN Orchestrator</span>
                </router-link>
            </div>
            <div class="nav-links">
                <router-link to="/" class="nav-link" exact-active-class="active"
                    >Dashboard</router-link
                >
                <router-link
                    :to="{ name: 'jobs', query: { status: 'processing' } }"
                    class="nav-link"
                    active-class="active"
                    >Jobs</router-link
                >
                <router-link
                    to="/resources"
                    class="nav-link"
                    active-class="active"
                    >Resources</router-link
                >
                <router-link
                    to="/syncs"
                    class="nav-link"
                    active-class="active"
                    >Syncs</router-link
                >
                <router-link
                    to="/datasets"
                    class="nav-link"
                    active-class="active"
                    >Datasets</router-link
                >
            </div>
        </nav>
        <main class="main-content">
            <router-view />
        </main>
    </div>
</template>

<style scoped>
.app {
    min-height: 100vh;
    background: #0f1117;
    color: #e4e4e7;
    font-family:
        system-ui,
        -apple-system,
        sans-serif;
}

.navbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 24px;
    height: 56px;
    background: #1a1d27;
    border-bottom: 1px solid #2a2d37;
}

.nav-brand {
    display: flex;
    align-items: center;
    gap: 8px;
}

.brand-icon {
    font-size: 20px;
}

.brand-text {
    font-size: 16px;
    font-weight: 600;
    letter-spacing: -0.3px;
}

.brand-link {
    display: flex;
    align-items: center;
    gap: 8px;
    color: inherit;
    text-decoration: none;
}

.brand-link:hover {
    opacity: 0.85;
}

.nav-links {
    display: flex;
    gap: 4px;
}

.nav-link {
    padding: 8px 16px;
    border-radius: 6px;
    color: #9ca3af;
    text-decoration: none;
    font-size: 14px;
    font-weight: 500;
    transition: all 0.15s ease;
}

.nav-link:hover {
    color: #e4e4e7;
    background: #2a2d37;
}

.nav-link.active {
    color: #fff;
    background: #3b82f6;
}

.main-content {
    max-width: 1200px;
    margin: 0 auto;
    padding: 24px;
}
</style>
