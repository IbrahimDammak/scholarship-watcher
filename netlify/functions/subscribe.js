const fs = require('fs');
const path = require('path');

const SUBSCRIBERS_PATH = path.join(__dirname, '..', '..', 'data', 'subscribers.json');

exports.handler = async (event, context) => {
    if (event.httpMethod !== 'POST') {
        return {
            statusCode: 405,
            body: JSON.stringify({ error: 'Method not allowed' })
        };
    }

    try {
        const body = parseBody(event.body, event.headers['content-type']);
        
        const email = body.email?.trim().toLowerCase();
        const countriesRaw = body.countries;
        const createdAt = body.created_at || new Date().toISOString();

        if (!email || !isValidEmail(email)) {
            return {
                statusCode: 400,
                body: JSON.stringify({ error: 'Invalid email address' })
            };
        }

        let countries = [];
        if (typeof countriesRaw === 'string') {
            try {
                countries = JSON.parse(countriesRaw);
            } catch {
                countries = countriesRaw.split(',').map(c => c.trim().toUpperCase()).filter(Boolean);
            }
        } else if (Array.isArray(countriesRaw)) {
            countries = countriesRaw.map(c => c.toUpperCase());
        }

        if (!countries || countries.length === 0) {
            return {
                statusCode: 400,
                body: JSON.stringify({ error: 'At least one country must be selected' })
            };
        }

        const subscriber = {
            email,
            countries,
            created_at: createdAt,
            active: true
        };

        const saved = await saveSubscriber(subscriber);

        if (saved) {
            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    success: true,
                    message: 'Subscription successful',
                    subscriber: {
                        email: subscriber.email,
                        countries: subscriber.countries
                    }
                })
            };
        } else {
            return {
                statusCode: 500,
                body: JSON.stringify({ error: 'Failed to save subscription' })
            };
        }

    } catch (error) {
        console.error('Subscription error:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: 'Internal server error' })
        };
    }
};

function parseBody(body, contentType) {
    if (!body) return {};
    
    if (contentType?.includes('application/json')) {
        return JSON.parse(body);
    }
    
    if (contentType?.includes('application/x-www-form-urlencoded')) {
        const params = new URLSearchParams(body);
        const result = {};
        for (const [key, value] of params) {
            result[key] = value;
        }
        return result;
    }
    
    try {
        return JSON.parse(body);
    } catch {
        return {};
    }
}

function isValidEmail(email) {
    if (!email || typeof email !== 'string') return false;
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
}

async function saveSubscriber(newSubscriber) {
    try {
        let data = { subscribers: [], last_updated: null, version: '1.0' };
        
        if (fs.existsSync(SUBSCRIBERS_PATH)) {
            const rawData = fs.readFileSync(SUBSCRIBERS_PATH, 'utf8');
            data = JSON.parse(rawData);
        }

        const existingIndex = data.subscribers.findIndex(
            s => s.email.toLowerCase() === newSubscriber.email.toLowerCase()
        );

        if (existingIndex >= 0) {
            const existing = data.subscribers[existingIndex];
            existing.countries = [...new Set([...existing.countries, ...newSubscriber.countries])];
            existing.active = true;
            existing.updated_at = new Date().toISOString();
        } else {
            data.subscribers.push(newSubscriber);
        }

        data.last_updated = new Date().toISOString();

        const dir = path.dirname(SUBSCRIBERS_PATH);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }

        fs.writeFileSync(SUBSCRIBERS_PATH, JSON.stringify(data, null, 2), 'utf8');
        
        console.log(`Subscriber saved: ${newSubscriber.email} -> ${newSubscriber.countries.join(', ')}`);
        return true;

    } catch (error) {
        console.error('Error saving subscriber:', error);
        return false;
    }
}
